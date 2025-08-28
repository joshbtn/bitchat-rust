use crate::{Error, Result};
use bluer::{
    Adapter, Address, Session,
    adv::{Advertisement, AdvertisementHandle},
    gatt::{
        local::{Application, ApplicationHandle, Characteristic, CharacteristicNotify, CharacteristicNotifyMethod, 
                CharacteristicWrite, CharacteristicWriteMethod, Service},
    },
};
use futures::{pin_mut, StreamExt};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;
use log::{debug, info, warn};
use std::time::Duration as StdDuration;

// DBus agent imports (used only when running on Linux with system bus)
#[cfg(target_os = "linux")]
use dbus_crossroads::Crossroads;
#[cfg(target_os = "linux")]
use dbus::blocking::Connection as DbusConnection;

// Service and characteristic UUIDs (matching iOS/Android)
pub const SERVICE_UUID: Uuid = Uuid::from_u128(0xF47B5E2D_4A9E_4C5A_9B3F_8E1D2C3A4B5C);
pub const CHARACTERISTIC_UUID: Uuid = Uuid::from_u128(0xA1B2C3D4_E5F6_4A5B_8C9D_0E1F2A3B4C5D);

pub type DataHandler = Arc<dyn Fn(Vec<u8>, String) + Send + Sync>;

#[derive(Clone)]
pub struct BluetoothConnectionManager {
    session: Arc<Session>,
    adapter: Arc<Adapter>,
    is_scanning: Arc<Mutex<bool>>,
    adv_handle: Arc<Mutex<Option<AdvertisementHandle>>>,
    app_handle: Arc<Mutex<Option<ApplicationHandle>>>,
    data_handler: Arc<Mutex<Option<DataHandler>>>,
    notifier: Arc<Mutex<Option<bluer::gatt::local::CharacteristicNotifier>>>,
    subscribed_centrals: Arc<RwLock<Vec<Address>>>,
}

impl BluetoothConnectionManager {
    pub async fn new() -> Result<Self> {
        let session = Session::new().await
            .map_err(|e| Error::Bluetooth(format!("Failed to create session: {}", e)))?;
            
        let adapter_names = session.adapter_names().await
            .map_err(|e| Error::Bluetooth(format!("Failed to get adapter names: {}", e)))?;
            
        let adapter_name = adapter_names.into_iter().next()
            .ok_or_else(|| Error::Bluetooth("No Bluetooth adapter found".to_string()))?;
            
        let adapter = session.adapter(&adapter_name)
            .map_err(|e| Error::Bluetooth(format!("Failed to get adapter: {}", e)))?;
            
        // Power on the adapter
        adapter.set_powered(true).await
            .map_err(|e| Error::Bluetooth(format!("Failed to power on adapter: {}", e)))?;
            
        // Configure adapter for connectionless operation
        // Try multiple approaches to disable pairing/bonding
        
        // 1. Disable pairing (may not be supported on all systems)
        if let Err(e) = adapter.set_pairable(false).await {
            warn!("Failed to disable pairing (may not be supported): {}", e);
        } else {
            info!("Successfully disabled pairing on adapter");
        }
        
        // 2. Set pairable timeout to 0 to make it non-pairable
        if let Err(e) = adapter.set_pairable_timeout(0).await {
            warn!("Failed to set pairable timeout (may not be supported): {}", e);
        } else {
            info!("Successfully set pairable timeout to 0");
        }
        
        // 3. Ensure discoverable is disabled to avoid system pairing prompts
        // Some BlueZ frontends will offer pairing dialogs when an adapter is
        // discoverable or discoverable timeout is set to 0. For our
        // connectionless BLE mesh we don't need the adapter to be discoverable
        // at the adapter level (we advertise actively), so try to disable it.
        if let Err(e) = adapter.set_discoverable(false).await {
            warn!("Failed to set discoverable (may not be supported): {}", e);
        } else {
            info!("Successfully disabled discoverable mode");
        }
        
        // 5. Try to disable legacy pairing and authentication
        // Note: These may not be supported on all systems but are worth trying
        info!("Configuring adapter for connectionless operation");
        
        // Log current adapter configuration for debugging
        if let Ok(powered) = adapter.is_powered().await {
            info!("Adapter powered: {}", powered);
        }
        if let Ok(pairable) = adapter.is_pairable().await {
            info!("Adapter pairable: {}", pairable);
        }
        if let Ok(discoverable) = adapter.is_discoverable().await {
            info!("Adapter discoverable: {}", discoverable);
        }
            
        Ok(Self {
            session: Arc::new(session),
            adapter: Arc::new(adapter),
            is_scanning: Arc::new(Mutex::new(false)),
            adv_handle: Arc::new(Mutex::new(None)),
            app_handle: Arc::new(Mutex::new(None)),
            data_handler: Arc::new(Mutex::new(None)),
            notifier: Arc::new(Mutex::new(None)),
            subscribed_centrals: Arc::new(RwLock::new(Vec::new())),
        })
    }
    
    pub async fn set_data_handler(&self, handler: DataHandler) {
        *self.data_handler.lock().await = Some(handler);
    }
    
    pub async fn start_advertising(&self) -> Result<()> {
        info!("Starting BLE advertising");
        
        // Advertise but avoid marking the adapter as globally discoverable.
        // Keep local name so peers can see the service, but don't force adapter-level
        // discoverable state which may trigger pairing flows in some environments.
        let advertisement = Advertisement {
            service_uuids: vec![SERVICE_UUID].into_iter().collect(),
            discoverable: Some(false),
            local_name: Some("BitChat".to_string()),
            ..Default::default()
        };
        
        let handle = self.adapter.advertise(advertisement).await
            .map_err(|e| Error::Bluetooth(format!("Failed to start advertising: {}", e)))?;
            
        *self.adv_handle.lock().await = Some(handle);
        
        Ok(())
    }
    
    pub async fn stop_advertising(&self) -> Result<()> {
        if let Some(handle) = self.adv_handle.lock().await.take() {
            drop(handle);
            info!("Stopped BLE advertising");
        }
        Ok(())
    }
    
    pub async fn start_gatt_server(&self) -> Result<()> {
        info!("Starting GATT server");
        
        let data_handler = self.data_handler.clone();
        let subscribed_centrals = self.subscribed_centrals.clone();
        
        let write_handle = CharacteristicWrite {
            // Prefer write without response to avoid triggering security/bonding
            // requirements on some platforms. Keep regular write as false so we
            // don't cause the stack to demand pairing for write access.
            write: false,
            write_without_response: true,
            method: CharacteristicWriteMethod::Fun(Box::new(move |new_value, req| {
                let data_handler = data_handler.clone();
                let subscribed_centrals = subscribed_centrals.clone();
                Box::pin(async move {
                    // Extract the device address from the request
                    let device_address = req.device_address;
                    let device_address_str = device_address.to_string();
                    debug!("Received {} bytes via GATT write from {}", new_value.len(), device_address_str);
                    
                    // Track this central
                    {
                        let mut centrals = subscribed_centrals.write().await;
                        if !centrals.contains(&device_address) {
                            centrals.push(device_address);
                            info!("Added {} to subscribed centrals list", device_address_str);
                            info!("Current subscribed centrals count: {}", centrals.len());
                        }
                    }
                    
                    if let Some(handler) = data_handler.lock().await.as_ref() {
                        handler(new_value, device_address_str);
                    }
                    
                    Ok(())
                })
            })),
            ..Default::default()
        };
        
        let notifier_ref = self.notifier.clone();
        let subscribed_centrals = self.subscribed_centrals.clone();
        
        let notify_handle = CharacteristicNotify {
            notify: true,
            method: CharacteristicNotifyMethod::Fun(Box::new(move |notifier| {
                let notifier_ref = notifier_ref.clone();
                let _subscribed_centrals = subscribed_centrals.clone();
                
                Box::pin(async move {
                    // Store the notifier for later use
                    *notifier_ref.lock().await = Some(notifier);
                    
                    // Track subscribed centrals
                    // Note: BlueR doesn't provide subscription events directly,
                    // but we can infer from write requests
                    info!("GATT characteristic notify handler initialized");
                })
            })),
            ..Default::default()
        };
        
        let service = Service {
            uuid: SERVICE_UUID,
            primary: true,
            characteristics: vec![
                Characteristic {
                    uuid: CHARACTERISTIC_UUID,
                    write: Some(write_handle),
                    notify: Some(notify_handle),
                    // Let the application authorize access; this gives us control
                    // over access requests at the application level instead of
                    // relying on BlueZ to enforce pairing/bonding.
                    authorize: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        
        let app = Application {
            services: vec![service],
            ..Default::default()
        };
        
        let app_handle = self.adapter.serve_gatt_application(app).await
            .map_err(|e| Error::Bluetooth(format!("Failed to serve GATT application: {}", e)))?;
            
        *self.app_handle.lock().await = Some(app_handle);
        
        Ok(())
    }
    
    pub async fn stop_gatt_server(&self) -> Result<()> {
        if let Some(handle) = self.app_handle.lock().await.take() {
            drop(handle);
            info!("Stopped GATT server");
        }
        Ok(())
    }
    
    pub async fn start_scanning(&self) -> Result<()> {
        let mut is_scanning = self.is_scanning.lock().await;
        if *is_scanning {
            return Ok(());
        }
        
        info!("Starting BLE scan");
        
        let filter = bluer::DiscoveryFilter {
            uuids: vec![SERVICE_UUID].into_iter().collect(),
            ..Default::default()
        };
        
        self.adapter.set_discovery_filter(filter).await
            .map_err(|e| Error::Bluetooth(format!("Failed to set discovery filter: {}", e)))?;
            
        self.adapter.set_powered(true).await
            .map_err(|e| Error::Bluetooth(format!("Failed to power on: {}", e)))?;
        
        // Discovery is started by calling discover_devices() in start_device_discovery_monitor()
            
        *is_scanning = true;
        info!("BLE scanning started successfully");
        Ok(())
    }
    
    pub async fn stop_scanning(&self) -> Result<()> {
        let mut is_scanning = self.is_scanning.lock().await;
        if !*is_scanning {
            return Ok(());
        }
        
        info!("Stopping BLE scan");
        
        // Discovery is stopped when the discover_devices stream is dropped
        
        *is_scanning = false;
        Ok(())
    }
    
    pub async fn send_data_via_advertisement(&self, data: &[u8]) -> Result<()> {
        info!("Sending data via BLE advertisement: {} bytes", data.len());
        
        // Stop current advertising
        self.stop_advertising().await?;
        
        // Create advertisement with data in the service data field
        let mut service_data = std::collections::HashMap::new();
        service_data.insert(SERVICE_UUID, data.to_vec());
        
        let advertisement = Advertisement {
            service_uuids: vec![SERVICE_UUID].into_iter().collect(),
            service_data: service_data.into_iter().collect(),
            // Avoid setting adapter-wide discoverable; advertising the service
            // is sufficient for BLE mesh discovery.
            discoverable: Some(false),
            local_name: Some("BitChat".to_string()),
            ..Default::default()
        };
        
        let handle = self.adapter.advertise(advertisement).await
            .map_err(|e| Error::Bluetooth(format!("Failed to start data advertising: {}", e)))?;
            
        *self.adv_handle.lock().await = Some(handle);
        
        // Keep advertising for a short time to ensure delivery
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        Ok(())
    }
    
    pub async fn process_discovered_device(&self, address: Address) -> Result<()> {
        info!("Processing discovered BitChat device: {}", address);
        
        // For connectionless approach, we just notify that a peer was discovered
        // The actual communication happens via advertisements and GATT server
        if let Some(handler) = self.data_handler.lock().await.as_ref() {
            // Send special peer discovery event
            handler(vec![0xFE, 0xFE], format!("DISCOVERED:{}", address));
        }
        
        Ok(())
    }
    
    pub async fn send_data(&self, _address: &Address, data: &[u8]) -> Result<()> {
        // In connectionless mode, we send data via GATT server notifications to all subscribed centrals
        // or via advertisements for broadcast messages
        debug!("Sending {} bytes via GATT server notifications", data.len());
        
        self.send_notification(data).await
    }
    
    pub async fn get_discovered_devices(&self) -> Result<Vec<Address>> {
        let addresses = self.adapter.device_addresses().await
            .map_err(|e| Error::Bluetooth(format!("Failed to get device addresses: {}", e)))?;
        debug!("Total discovered devices: {}", addresses.len());
            
        let mut filtered = Vec::new();
        
        for addr in addresses {
            if let Ok(device) = self.adapter.device(addr) {
                // Get device name for debugging
                let name = device.name().await.ok().flatten().unwrap_or_else(|| "Unknown".to_string());
                debug!("Checking device {} ({})", addr, name);
                
                // Check if device advertises our service
                if let Ok(uuids) = device.uuids().await {
                    if let Some(uuids) = uuids {
                        debug!("Device {} advertises {} UUIDs", addr, uuids.len());
                        if uuids.contains(&SERVICE_UUID) {
                            info!("Device {} ({}) advertises BitChat service", addr, name);
                            filtered.push(addr);
                        }
                    } else {
                        debug!("Device {} has no advertised UUIDs", addr);
                    }
                } else {
                    // Also check service data for our service UUID
                    if let Ok(Some(service_data)) = device.service_data().await {
                        if service_data.contains_key(&SERVICE_UUID) {
                            info!("Device {} ({}) has BitChat service data", addr, name);
                            filtered.push(addr);
                        }
                    }
                }
            }
        }
        
        Ok(filtered)
    }
    
    pub async fn get_active_peer_count(&self) -> usize {
        // In connectionless mode, return the number of subscribed centrals
        self.subscribed_centrals.read().await.len()
    }
    
    pub async fn is_peer_active(&self, _address: &Address) -> bool {
        // In connectionless mode, we consider peers active if they're recently discovered
        // This is a simplified implementation - in practice you might want to track
        // recent advertisement timestamps
        true
    }
    
    pub async fn start_device_discovery_monitor(&self) -> Result<()> {
        info!("Starting device discovery monitor");
        let device_events = self.adapter.discover_devices().await
            .map_err(|e| Error::Bluetooth(format!("Failed to start device discovery: {}", e)))?;
            
        let adapter = self.adapter.clone();
        let connection_manager = self.clone();
        
        tokio::spawn(async move {
            pin_mut!(device_events);
            info!("Device discovery monitor task started");
            
            while let Some(device_event) = device_events.next().await {
                debug!("Device event received: {:?}", device_event);
                match device_event {
                    bluer::AdapterEvent::DeviceAdded(addr) => {
                        info!("Device discovered: {}", addr);
                        
                        // Check if it advertises our service
                        if let Ok(device) = adapter.device(addr) {
                            debug!("Got device object for {}", addr);
                            
                            // Check for service data first (for data messages)
                            if let Ok(Some(service_data)) = device.service_data().await {
                                if let Some(data) = service_data.get(&SERVICE_UUID) {
                                    info!("Received data via advertisement from {}: {} bytes", addr, data.len());
                                    
                                    // Process the received data
                                    if let Some(handler) = connection_manager.data_handler.lock().await.as_ref() {
                                        handler(data.clone(), addr.to_string());
                                    }
                                }
                            }
                            
                            // Also check for service UUID announcements
                            match device.uuids().await {
                                Ok(Some(uuids)) => {
                                    debug!("Device {} UUIDs: {:?}", addr, uuids);
                                    if uuids.contains(&SERVICE_UUID) {
                                        info!("Found BitChat device: {} - processing discovery", addr);
                                        
                                        // Process discovery instead of connecting
                                        if let Err(e) = connection_manager.process_discovered_device(addr).await {
                                            warn!("Failed to process discovered device {}: {}", addr, e);
                                        }
                                    } else {
                                        debug!("Device {} does not advertise BitChat service", addr);
                                    }
                                }
                                Ok(None) => {
                                    debug!("Device {} has no UUIDs", addr);
                                }
                                Err(e) => {
                                    debug!("Failed to get UUIDs for device {}: {}", addr, e);
                                }
                            }
                        } else {
                            debug!("Failed to get device object for {}", addr);
                        }
                    }
                    bluer::AdapterEvent::DeviceRemoved(addr) => {
                        info!("Device removed: {}", addr);
                        // In connectionless mode, just notify about device removal
                        if let Some(handler) = connection_manager.data_handler.lock().await.as_ref() {
                            handler(vec![0xFD, 0xFD], format!("REMOVED:{}", addr));
                        }
                    }
                    _ => {
                        debug!("Other device event: {:?}", device_event);
                    }
                }
            }
            
            warn!("Device discovery monitor task ended");
        });
        
        Ok(())
    }
    
    pub async fn get_adapter(&self) -> Arc<Adapter> {
        self.adapter.clone()
    }
    
    /// Try to reject pairing requests to avoid unwanted pairing dialogs
    pub async fn reject_pairing_requests(&self) -> Result<()> {
        // This method could be used to monitor for and reject pairing requests
        // The implementation would depend on BlueZ D-Bus API access for pairing events
        info!("Setting up pairing request rejection (if supported)");
        
        // On Linux, try to register a DBus Agent with BlueZ that rejects
        // any pairing/bonding requests. This prevents desktop environments
        // from showing pairing dialogs when peers connect for GATT access.
        #[cfg(target_os = "linux")]
        {
            // Spawn a blocking thread to register a DBus agent and serve requests
            let _ = std::thread::spawn(move || {
                // Use a short timeout when calling BlueZ
                let timeout = StdDuration::from_secs(5);

                match DbusConnection::new_system() {
                    Ok(conn) => {
                        let mut cr = Crossroads::new();

                        // Register org.bluez.Agent1 interface with methods that reject
                        let iface_token = cr.register("org.bluez.Agent1", |b| {
                            b.method("Release", (), (), |_, _, _: ()| {
                                Ok(())
                            });

                            b.method("RequestPinCode", ("device",), ("pin",), |_, _, (_device,): (dbus::Path<'_>,)| -> std::result::Result<(String,), dbus_crossroads::MethodErr> {
                                std::result::Result::Err(dbus_crossroads::MethodErr::failed("Rejected by BitChat agent"))
                            });

                            b.method("DisplayPinCode", ("device","pincode"), (), |_, _, _: (dbus::Path<'_>, String)| {
                                // Ignore display requests
                                Ok(())
                            });

                            b.method("RequestPasskey", ("device",), ("passkey",), |_, _, (_device,): (dbus::Path<'_>,)| -> std::result::Result<(u32,), dbus_crossroads::MethodErr> {
                                std::result::Result::Err(dbus_crossroads::MethodErr::failed("Rejected by BitChat agent"))
                            });

                            b.method("DisplayPasskey", ("device","passkey","entered"), (), |_, _, _: (dbus::Path<'_>, u32, u16)| {
                                Ok(())
                            });

                            b.method("RequestConfirmation", ("device","passkey"), (), |_, _, (_device, _passkey): (dbus::Path<'_>, u32)| -> std::result::Result<(), dbus_crossroads::MethodErr> {
                                std::result::Result::Err(dbus_crossroads::MethodErr::failed("Rejected by BitChat agent"))
                            });

                            b.method("AuthorizeService", ("device","uuid"), (), |_, _, (_device, _uuid): (dbus::Path<'_>, String)| -> std::result::Result<(), dbus_crossroads::MethodErr> {
                                std::result::Result::Err(dbus_crossroads::MethodErr::failed("Rejected by BitChat agent"))
                            });

                            b.method("Cancel", (), (), |_, _, _: ()| {
                                Ok(())
                            });
                        });

                        // Export at a known object path
                        let path = dbus::Path::new("/org/bluez/bitchat/agent").unwrap();
                        cr.insert(path.clone(), &[iface_token], ());

                        // Register with BlueZ AgentManager1
                        let proxy = conn.with_proxy("org.bluez", "/org/bluez", timeout);
                        let capability: &str = "NoInputNoOutput";
                        let _register_result: std::result::Result<(), dbus::Error> = proxy.method_call("org.bluez.AgentManager1", "RegisterAgent", (path.clone(), capability));
                        let _default_result: std::result::Result<(), dbus::Error> = proxy.method_call("org.bluez.AgentManager1", "RequestDefaultAgent", (path.clone(),));

                        // Serve incoming DBus messages (blocking)
                        if let Err(e) = cr.serve(&conn) {
                            warn!("DBus agent serve failed: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to connect to system bus for pairing agent: {}", e);
                    }
                }
            });
        }

        Ok(())
    }
    
    pub async fn send_notification(&self, data: &[u8]) -> Result<()> {
        let mut notifier_guard = self.notifier.lock().await;
        if let Some(notifier) = notifier_guard.as_mut() {
            debug!("Sending notification with {} bytes to subscribed centrals", data.len());
            
            // Send notification to all subscribed centrals
            notifier.notify(data.to_vec())
                .await
                .map_err(|e| Error::Bluetooth(format!("Failed to send notification: {}", e)))?;
                
            Ok(())
        } else {
            // No notifier means no subscribed centrals
            debug!("No subscribed centrals to notify");
            Ok(())
        }
    }
    
    pub async fn has_subscribed_centrals(&self) -> bool {
        // Check if we have any subscribed centrals (not just if notifier exists)
        !self.subscribed_centrals.read().await.is_empty()
    }
    
    pub async fn get_subscribed_centrals_count(&self) -> usize {
        self.subscribed_centrals.read().await.len()
    }
    
    /// Return a list of currently discovered addresses that look like peers.
    /// This is used by higher-level code to iterate and perform actions such as
    /// sending data or disconnecting. We return Addresses discovered by the
    /// adapter that advertise our service or contain service data for it.
    pub async fn get_connected_addresses(&self) -> Vec<Address> {
        match self.get_discovered_devices().await {
            Ok(addrs) => addrs,
            Err(_) => Vec::new(),
        }
    }

    /// Disconnect a peer by address. In this connectionless implementation
    /// there may not be a connected link to tear down; attempt to call the
    /// device disconnect if available and remove any tracked subscribed central
    /// entries.
    pub async fn disconnect_from_device(&self, address: &Address) -> Result<()> {
        info!("disconnect_from_device requested for {}", address);

        // Try to get a device object and call disconnect if supported by BlueZ
        if let Ok(device) = self.adapter.device(*address) {
            // Attempt to call disconnect; if the API isn't present or fails,
            // log and continue.
            if let Err(e) = device.disconnect().await {
                warn!("Failed to disconnect device {}: {}", address, e);
            } else {
                info!("Called disconnect() on device {}", address);
            }
        } else {
            debug!("No device object available for {} when disconnecting", address);
        }

        // Remove from subscribed centrals if present
        {
            let mut centrals = self.subscribed_centrals.write().await;
            centrals.retain(|a| a != address);
        }

        Ok(())
    }
}
