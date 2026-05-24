# ioBroker.fid-smartlife

Tuya / Smart Life cloud + local LAN adapter for ioBroker.

> ⚠️ **ALPHA / EXPERIMENTAL** ⚠️
>
> This adapter is in active development on a single production system.
> Breaking changes between versions are expected. There is no support
> guarantee. Issues are welcome but response time is voluntary. Use at
> your own risk.

## Features

- Reads device list, schemas and live status from the Tuya OpenAPI v2
- Local LAN control (Tuya protocol 3.3) — most write commands run without
  hitting the cloud, no quota burn even for blink scripts
- LAN-Discovery picks up device IPs automatically from broadcast announces
- Cloud-IP-Import command as fallback when discovery doesn't catch a device
- Online state based on actual local reachability with hysteresis (50 fails
  or 60 minutes without local-OK before flipping to offline) — battery /
  sub-devices fall back to cloud-reported online state
- Optional Pulsar/MQTT push subscriber for cloud status events (experimental,
  off by default)
- Schema database with ~8000 entries as fallback for devices where the cloud
  spec is sparse
- Bitmap, RGB-color and energy-plug enhanced derived states (compatible with
  the iobroker.tuya state layout: state under both code-name and DPS-id)

## Requirements

1. A Tuya Smart / Smart Life account (the same one used by the mobile app)
2. A Tuya IoT Platform cloud project (free, see "Tuya account setup" below)
3. ioBroker with js-controller 5+ and Node.js 18+

## Tuya account setup

You need a Tuya Developer cloud project with your Smart Life app account
linked. This is the same procedure as for `iobroker.tuya`'s cloud mode.

1. Sign in at https://platform.tuya.com (or `iot.tuya.com`)
2. **Cloud → Development → Create Cloud Project**
   - Name: anything you like
   - Industry: *Smart Home*
   - Development Method: *Smart Home*
   - Data Center: pick the one matching your Smart Life region (Central
     Europe → EU, North America → US, ...)
3. After creating, the project shows **Access ID** and **Access Secret** —
   copy both.
4. **Service API** tab → make sure these API services are subscribed and
   authorized to the project:
   - **IoT Core** (required)
   - **Authorization** (required)
   - **Smart Home Basic Service** (required)
   - **Device Status Notification** (only needed for the optional Pulsar
     push subscriber)
5. **Devices → Link Tuya App Account → Add App Account** — scan the QR with
   your Smart Life app. After confirming, your devices appear under
   *All Devices*.

If you have a Tuya **Trial Edition** (the default for new accounts), the
"controllable device pool quota" is limited — typically renewed monthly.
With many devices, prefer local control (which fid-smartlife uses by
default) to keep cloud calls minimal.

## Installation

### Option A: via Admin UI (recommended for testing)

In ioBroker admin: *Adapters → Install via URL* and enter:

```
https://github.com/fiducerion/ioBroker.fid-smartlife
```

ioBroker pulls the latest commit on `main` and installs it.

### Option B: via CLI

```bash
iobroker url https://github.com/fiducerion/ioBroker.fid-smartlife
```

### Option C: pin a specific release tag

```bash
iobroker url https://github.com/fiducerion/ioBroker.fid-smartlife/tarball/v0.7.7
```

## Configuration

Open the instance settings in the admin UI:

- **Access ID** + **Access Secret** + **Region**: from the Tuya cloud project
- **Polling interval**: how often to refresh status from devices (default 60s).
  Lower for faster updates, but more cloud calls.
- **Enable LAN discovery**: should stay on — without it no local IPs
- **Enable Pulsar/MQTT Push** (experimental): subscribe to device status
  push from Tuya cloud. Default off. Requires "Device Status Notification"
  API in the cloud project.

After saving, the adapter discovers all devices from the cloud and creates
states under:

```
fid-smartlife.0.<deviceId>.<dpId>          # primary state, matches iobroker.tuya layout
fid-smartlife.0.<deviceId>.<codeName>      # alias for the same data point
fid-smartlife.0.<deviceId>.ip              # local IP (set by LAN-Discovery)
fid-smartlife.0.<deviceId>.localKey        # AES key for local control
fid-smartlife.0.<deviceId>.online          # local reachability with hysteresis
fid-smartlife.0.<deviceId>.noLocalConnection  # set to true to disable local writes
fid-smartlife.0.<deviceId>._noCloudStatusPoll # set to true to skip cloud polling
```

Plus diagnostic states under `fid-smartlife.0.info.*`:

```
info.connection            # adapter healthy
info.cloudQuotaPaused      # Tuya quota guard active (60min backoff)
info.pulsarConnected       # Pulsar/MQTT push subscriber active
info.pulsarMessages        # decrypted push messages counter
info.localResetV065        # one-time migration marker (don't touch)
```

## Diagnostic sendto commands

```bash
# How many writes went local vs cloud, plus list of devices without local IP
iobroker sendto fid-smartlife.0 writeStats

# Import IPs from cloud for devices that LAN discovery hasn't caught
iobroker sendto fid-smartlife.0 importCloudIPs

# Find devices that used to be in the cloud but aren't anymore
iobroker sendto fid-smartlife.0 findMissingDevices

# Raw device list from Tuya cloud
iobroker sendto fid-smartlife.0 listDevicesRaw
```

## Heavy switching scripts (alarm blink, etc)

If you have scripts that rapidly toggle devices (e.g. blink lights every
3 seconds during an alarm), make sure those devices:

1. Have a `local.ip` set (check with `iobroker state get
   fid-smartlife.0.<deviceId>.ip`)
2. Are reachable from the ioBroker host (no VLAN/firewall isolation)
3. Optionally have `_noCloudStatusPoll = true` to skip the periodic cloud
   status read

If `local.ip` is empty, run `importCloudIPs` once. After that the adapter
writes locally and burns no cloud quota for these scripts.

## Internet block as quota protection

A nice trick: block internet access to specific Tuya devices in your
router (e.g. AVM FRITZ!Box → Network → Devices → tick "deny internet").
The device stays reachable locally, broadcasts its presence to the LAN,
and `fid-smartlife` controls it via the local protocol. Tuya cloud will
report the device as offline (which is fine — the local online state
based on reachability is authoritative). No more accidental cloud calls
for that device, ever.

## What is NOT supported (yet)

- Tuya protocol v3.4 (HMAC-SHA256 handshake + session key) — affects a
  small number of newer devices, they fall back to cloud-only
- Camera streams (RTSPS allocation, snapshot URLs)
- Tuya Bluetooth gateway sub-devices via local protocol (cloud only)
- Multi-instance setups

## License

MIT
