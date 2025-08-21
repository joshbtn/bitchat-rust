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
        
        let advertisement = Advertisement {
            service_uuids: vec![SERVICE_UUID].into_iter().collect(),
            discoverable: Some(true),
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
            write: true,
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
            discoverable: Some(true),
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
    
    pub fn get_adapter(&self) -> Arc<Adapter> {
        self.adapter.clone()
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
}
