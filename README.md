# Bridge watch

Sends an [ntfy](https://ntfy.sh) notification about an hour before a large ship
passes under the Ambassador Bridge (Detroit / Windsor), saying which direction
it is coming from.

Ship positions come from the free [aisstream.io](https://aisstream.io) feed.
A GitHub Actions schedule runs `bridge-watch.js --once` twice an hour: it
listens to the feed for 25 seconds, works out each large ship's time to the
bridge, and asks ntfy to deliver the alert 60 minutes before that time.

## Setup

1. Subscribe to your ntfy topic in the ntfy phone app.
2. Add two repository secrets under Settings > Secrets and variables > Actions:
   - `AISSTREAM_API_KEY`: your aisstream.io key
   - `NTFY_TOPIC`: your ntfy topic name
3. The schedule in `.github/workflows/bridge-watch.yml` then runs on its own.
   Use "Run workflow" on the Actions tab to try it straight away.

## Settings

Set these as `env` values in the workflow, or in `config.json` when running
locally.

| Setting | Default | Meaning |
|---|---|---|
| `MIN_LENGTH_M` | 227 | Smallest ship to alert on, in metres. Seaway-size ships report 225 or 226. |
| `LEAD_MINUTES` | 60 | How long before the bridge the alert arrives. |
| `LISTEN_SECONDS` | 25 in the cloud | How long each check listens to the feed. |
| `LOOKAHEAD_MINUTES` | 40 in the cloud | How far past the lead time alerts are scheduled, to cover late runs. |

## Running it yourself

Needs Node 22 or newer and no packages.

```
node bridge-watch.js --init         create config.json with a new ntfy topic
node bridge-watch.js --test-notify  send a test notification
node bridge-watch.js --selftest     check the timing logic with simulated ships
node bridge-watch.js --once         one check, as the cloud schedule does
node bridge-watch.js                stay connected and alert in real time
```

## Limits

- A ship that docks or anchors after its alert was scheduled still triggers it.
- A ship that leaves a dock less than an hour from the bridge gets a
  short-notice alert, or none if it passes between two checks.
- GitHub can start scheduled runs late, and pauses schedules on a repo with no
  activity for 60 days.
- The channel line and speed assumptions are approximate; expect the predicted
  time to be within a few minutes.
