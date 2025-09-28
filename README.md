# UPBeat Hubitat Drivers for Universal Powerline Bus (UPB)
![GitHub Downloads (all assets, all releases)](https://img.shields.io/github/downloads/UPBeat-Automation/UPBeat-Hubitat/total)

The Universal Powerline Bus (UPB) is a communication protocol used for home automation and building control. It enables devices to communicate over the existing powerline infrastructure, eliminating the need for additional wiring. UPB is a robust, reliable, and scalable technology designed for applications such as lighting control, HVAC management, and appliance integration. Through testing, field trials, and residential installations, UPB has been proven to be greater than 99.9% reliable.

The Universal Powerline Bus (UPB) Hubitat Integration allows you to control your UPB devices via Alexa, Google Assistant, and Apple HomeKit.

## Minimum Requirements

1. Hubitat Elevation® Model C-7 or C-8 controller [Click here to purchase for Hubitat Elevation](https://hubitat.com)
2. Pulseworx PIM-U Powerline Interface Module [Click here to purchase PIM-U](https://pcswebstore.com/products/pulseworx-powerline-interface-module-usb)
3. Linux device running net2ser eg. Raspbery Pi [Click here to purchase Raspberry Pi](https://www.raspberrypi.com/products/raspberry-pi-5)
4. Download the UPBeat Hubitat Drivers [Click here to download driver bundle](https://github.com/xeren-io/UPBeat-Hubitat/releases)

## Recommended Setup for Power Over Ethernet (POE)
1. Hubitat Elevation® Model C-7 or C-8 controller [Click here to purchase for Hubitat Elevation](https://hubitat.com)
2. POE micro USB Adapter for Model C-7 [Click here to purchase micro USB POE adapter](https://www.amazon.com/dp/B01MDLUSE7) 
3. Pulseworx PIM-U Powerline Interface Module [Click here to purchase PIM-U](https://pcswebstore.com/products/pulseworx-powerline-interface-module-usb)
4. Linux device running net2ser eg. Raspbery Pi [Click here to purchase Raspberry Pi](https://www.raspberrypi.com/products/raspberry-pi-5)
5. Rapberry Pi Case [Click here to buy official Pi 5 case](https://www.digikey.com/en/products/detail/raspberry-pi/SC1160/21658256)
6. Raspberry Pi POE Hat [Click here to purchase Raspberry Pi POE Hat](https://www.amazon.com/dp/B0D7SDGXKL)
7. Download the UPBeat Hubitat Drivers [Click here to download driver bundle](https://github.com/xeren-io/UPBeat-Hubitat/releases)

## Installing Ser2Net for Ubuntu / Raspberry Pi

1. Raspbian (Raspberry Pi OS Lite) [Installing Raspberry Pi OS](https://www.raspberrypi.com/documentation/computers/getting-started.html)
2. Once installed, use apt to install Ser2Net, you should be able to cut and paste the following commands:

```bash
sudo apt update
sudo apt install ser2net
```

3. As root, using a text editor, modify the /etc/ser2net.yaml. The only entry that is needed in the full file is shown below.
   
#### Raspberry Pi example with Pulseworx PIM-U Powerline Interface Module:
```yaml
%YAML 1.1
---
# This is a ser2net configuration file, tailored to be rather
# simple.
#
# Find detailed documentation in ser2net.yaml(5)
# A fully featured configuration file is in
# /usr/share/doc/ser2net/examples/ser2net.yaml.gz
#
# If you find your configuration more useful than this very simple
# one, please submit it as a bugreport

connection: &UPBPIM
    accepter: tcp,4999
    enable: on
    options:
      kickolduser: true
      telnet-brk-on-sync: false
    connector: serialdev,
              /dev/ttyUSB0,
              4800n81,local
```

4. Once you have saved the changes to the /etc/ser2net.yaml file you will need to restart the ser2net service. Using the following commands.
```
sudo service ser2net stop
sudo service ser2net start
```

5. Check the service status using `sudo service ser2net status` you should see somthing like shown below.
   
![image](https://github.com/user-attachments/assets/089a4e92-1e42-4d09-9b16-2b4ff1081f9a)

## Installing Hubitat Drivers

1. Download the drivers bundle from [releases](https://github.com/UPBeat-Automation/UPBeat-Hubitat/releases)
   ![image](https://github.com/user-attachments/assets/97423b7e-5bc6-474e-8ad6-54f60a3c5d81)

3. Navigate to your Hubitat Controller's web interface and go to the "FOR DEVELOPERS" --> Bundles
   
   ![image](https://github.com/user-attachments/assets/0f0db16c-ba8c-4802-bbbd-605646350e8f)

4. Import the "UPBeatUniversalPowerlineBusIntegration.zip" into your Hubitat controller.
   
   ![image](https://github.com/user-attachments/assets/f1116aab-b6de-4126-a921-10dc458247e7)

5. Once the import is completed, you should see the following.

   ![image](https://github.com/user-attachments/assets/9ad2b6ce-b4f6-4d27-a4df-9506e79a635a)

## Configuring the UPBeat App

1. Navigate to your Hubitat Controller's web interface and go to the Apps section.

   ![image](https://github.com/user-attachments/assets/4607aab7-8a7c-4c2b-80e6-3e90ade1644b)

2. Click "Add user app" and click UPBeat App.

   ![image](https://github.com/user-attachments/assets/c93fa1b9-cb70-4060-a079-eb664cf13bb8)

3. Click "Done" on the app page.

   ![image](https://github.com/user-attachments/assets/5614d0ee-26e9-46af-9105-9a03f61df6fe)

## Configuring the UPBeat Powerline Interface Module
1. Navigate to your Hubitat Controller's web interface and go to the Devices section.
   
   ![image](https://github.com/user-attachments/assets/7b580f05-8354-4fbf-b701-83d580ddf783)

3. Edit the UPB Powerline Interface Module device, goto the Preferences tab. Set the IP and port of your ser2net device from earlier.
   
   ![image](https://github.com/user-attachments/assets/023a1565-d57e-41cb-838f-446cedd8d1a3)

5. Save, the setting and go to the Commands tab. You should see the following.
   
   ![image](https://github.com/user-attachments/assets/7894b0e6-d81d-4eb8-9c63-e49db8488709)

   The PIM driver is a raw socket driver, sometimes you will see the following status.  
   ![image](https://github.com/user-attachments/assets/2329c3b1-9a1f-4628-8af3-ff6b43fb8c5c)

   It's completely normal and comes from the socket driver when no data arrives in a minute.

## Adding devices manually
1. Navigate to your Hubitat Controller's web interface and go to the Apps section. Click on UPBeat App.
   
   ![image](https://github.com/user-attachments/assets/c443e427-3bda-4a55-b13e-88fa6db475bc)

3. Click on the "Manually Add Device", select a device type. as shown below.

   ![image](https://github.com/user-attachments/assets/42aa46ee-807f-4030-a5a1-c58cf10b56f7)

4. Set the Name, Voice Name, UPB Network Id, and UPB Device Id, click Next.

   ![image](https://github.com/user-attachments/assets/0721d3a0-90fa-4f5b-8d31-813755452313)

5. Click the Go to Device Page button.

   ![image](https://github.com/user-attachments/assets/89e40ae4-7950-4861-9087-8160c1b4c897)

6. You should now be able to control your device.

## Adding devices via UpStart UPE import

1. Navigate to your Hubitat Controller's web interface and go to the Apps section. Click on UPBeat App.
   
   ![image](https://github.com/user-attachments/assets/c443e427-3bda-4a55-b13e-88fa6db475bc)

2. Select Bulk Import and paste UPE file contents. Then click Next.

   ![image](https://github.com/user-attachments/assets/9b0089fe-680c-40d0-b4c1-556fe02a78d2)

   Review the import results, and click Next. 

   ![image](https://github.com/user-attachments/assets/6ba492c0-0c04-4865-8f7e-fce7e7641d03)

   You are ready to go... 🥇

   ![image](https://github.com/user-attachments/assets/616afa43-5e8e-46a7-98ed-12c7ac099878)

## Amazon Alexa Integrated Devices

The following section shows a view of the devices from Alexa app.  

![image](https://github.com/user-attachments/assets/f7bd83b4-b855-418b-8cd7-d678fe01bed3)

## Driver Notes

The primary drivers needed for most UPB deployments are `UPB Scene Switch`, `UPB Dimming Switch`, and `UPB Non-Dimming Switch` but other experimental drivers exist.

The virtual `UPB Multi-Speed Fan Driver`, `UPB Single-Speed Fan Driver` and `UPB Actuator Driver` work in hubitat but are not fully tested against Alexa, Google, and Apple home assistants. 

This is a current area of testing and development. You can change the driver associated with the respective device types from the device info view.

## Limitations

The UPBeat UI is able to parse the complete UPE file, however; some device types are not currently supported, please see the list below for more information: 

**Note: Any devices which trigger scenes will be propagated to show the correct device states. Also multi-channel devices are supported currently by creating a device per channel. In the future, parent and child devices may be used to reflect a parent device and child channels.**

- 0 = Other (Unsupported)
- 1 = Keypad (Partial) - Keypads scene triggers will be propagated to show the correct device states.
- 2 = Switch (On/Off/Dim/Status)
- 3 = Module (On/Off/Dim/Status)
- 4 = Input Module (Unsupported)
- 5 = Input-Output Module (Unsupported)
- 6 = VPM (Unsupported)
- 7 = VHC (Unsupported)
- 8 = Thermostat (Unsupported)
- 9 = XPW (Unsupported)
- 10 = RFI (Unsupported)

We hope to grow device support with the help of the community and other developers. 

## License
This code is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-nc-sa/4.0/).  
Copyright (c) 2025 UPBeat Automation.  
You may use, modify, and share this code for non-commercial purposes, provided you credit UPBeat Automation and distribute derivatives under the same license. Selling this code or its derivatives is prohibited.

## Contributing
Contributions are welcome! Please submit pull requests or open issues for bugs and feature requests. All contributions must comply with the license terms.

## Help and Support

If you find any bugs please feel to open a bug report and if you have any questions feel free to reach out. 
