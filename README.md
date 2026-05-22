# iobroker.fid-smartlife

Cloud-First Tuya / Smartlife Adapter, Teil von **Fiducerion Core**.

Portiert vom User-Skript `TuyaCloudReplace v2.5.2`.

## Status

**v0.1.0** - Iteration 1. Funktioniert ueber die Tuya Cloud (OpenAPI v2).
Lokales Protokoll (Tuya LAN v3.3) ist fuer v0.2.0 geplant.

## Voraussetzungen

1. Account bei https://iot.tuya.com (kostenlos)
2. Neues Cloud-Projekt anlegen, eigenen Smartlife/Tuya-App-Account linken
3. Access ID + Access Secret kopieren
4. Region passend zum App-Account waehlen (EU / US / CN / IN)

## Konfiguration

Im Admin: Access ID + Access Secret + Region eintragen, speichern. Der Adapter
holt dann automatisch alle Geraete und legt States an unter:

```
fid-smartlife.0.<deviceId>.<dpCanon>
fid-smartlife.0.<deviceId>.on            # Alias falls ableitbar
fid-smartlife.0.<deviceId>.brightness    # Alias
fid-smartlife.0.<deviceId>.color_temp_k  # Alias
...
```

## Bekannte zickige Geraete

In der Adapter-Konfig unter "Geraete-IDs ohne Cloud-Status-Poll" stehen
komma-getrennte deviceIds, fuer die das regelmaessige Status-Polling
deaktiviert ist (z.B. weil Tuya `function not support` zurueckmeldet).

Wird automatisch erweitert wenn der Adapter zur Laufzeit ein solches Geraet
erkennt - der entsprechende `_noCloudStatusPoll`-Switch unter dem Geraet
wird auf `true` gesetzt.

## Migration vom TuyaCloudReplace-Skript

Der Adapter legt States unter `fid-smartlife.0.<id>.*` an, nicht unter
`0_userdata.0.Geraete.Tuya.*`. Damit kollidiert nichts.

1. Adapter installieren, konfigurieren, starten
2. Pruefen ob alle gewuenschten Geraete unter `fid-smartlife.0.*` ankommen
3. Konsumenten (Skripte, LCARS-App, vis-Views) umstellen
4. Wenn alles laeuft: TuyaCloudReplace-Skript deaktivieren, alte DPs unter
   `0_userdata.0.Geraete.Tuya.*` loeschen

## Lizenz

MIT
