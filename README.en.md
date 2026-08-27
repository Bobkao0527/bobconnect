# BobConnect

> [繁體中文版本](README.md)

BobConnect is a browser-based peer-to-peer file transfer tool built with WebRTC. File contents are transferred directly between two browsers. Cloudflare Durable Objects are used only for pairing and WebRTC signaling; they do not relay file contents.

## AI Collaborators

- GPT 5.6 Luna
- GPT 5.3
- Gemini 3.7 flash
- Gemini 3.6 flash
- Ox-Alpha (GLM 5.3 flash)

## Features

- QR Code and six-digit PIN pairing
- Nearby-device discovery using the public IPv4 address
- Encrypted peer-to-peer transfer through WebRTC DataChannels
- Chunked large-file transfer with flow control and resume support
- OPFS temporary storage for received files to reduce memory pressure
- Mobile camera scanning and Wake Lock support
- Durable Object signaling connection and message limits
- Content Security Policy and same-origin API protection

## Architecture

```text
Browser A                         Browser B
    |                                 |
    |------ WebRTC DataChannel -------|
    |                                 |
    +---------- Pages frontend -------+
                    |
              /api WebSocket
                    |
             Pages Function
                    |
          SIGNAL_ROOM binding
                    |
          Cloudflare Worker
                    |
        Durable Object SignalRoom
```

- `public/`: Frontend HTML, CSS, JavaScript, and the OPFS Worker
- `functions/api/[[path]].js`: Cloudflare Pages Function API entry point
- `do-worker.js`: Separately deployed Cloudflare Worker and Durable Object class
- `makedo.toml`: Wrangler configuration for the Durable Object Worker

The frontend uses the relative `/api` path from the current page origin. Each deployment therefore connects to its own Pages Function and Durable Object.

## Requirements

- A Cloudflare account
- Node.js and npm
- Wrangler CLI
- A Cloudflare Pages project
- A `SIGNAL_ROOM` Durable Object binding for the Pages Function

Cloudflare API tokens, R2 secrets, passwords, and other credentials are not required in the source code. This project currently does not use R2 for file transfer.

## Deploy Your Own Instance

### 1. Get the source code

```bash
git clone <your-repository-url>
cd bobconnect
```

### 2. Install and log in to Wrangler

```bash
npm install -g wrangler
wrangler login
```

Alternatively, use the local package runner:

```bash
npx wrangler login
```

### 3. Deploy the Durable Object Worker

```bash
npx wrangler deploy -c makedo.toml
```

This creates or updates:

- Worker: `bobconnect-do`
- Durable Object class: `SignalRoom`
- Durable Object binding: `SIGNAL_ROOM`

If the first deployment reports a Durable Object migration issue, verify that the migration in `makedo.toml` is being applied to the intended Cloudflare account and environment.

### 4. Configure the Pages Durable Object binding

In the Cloudflare Dashboard, open your Pages project:

1. Open **Settings**.
2. Open the **Functions** or **Bindings** settings.
3. Add a Durable Object binding.
4. Set the binding name to `SIGNAL_ROOM`.
5. Select the `SignalRoom` Durable Object namespace from your own Worker deployment.
6. Save the configuration and verify it is configured for the Production environment, not only Preview.

The binding name must be exactly `SIGNAL_ROOM` because the Pages Function reads `env.SIGNAL_ROOM`.

### 5. Deploy Pages

In the Pages project Git settings, use:

- **Framework preset**: None or Static HTML
- **Build command**: `npm run build`
- **Build output directory**: `public`

The repository includes the built `public/tailwind.css`, so the static frontend can also be served directly. Pages still runs the build command to regenerate it from the current source. For custom CI, run:

```bash
npm install
npm run build
```

Deploy the repository through Pages. Pages automatically deploys the `functions/` directory as Pages Functions and serves the frontend from `public/`.

If you use Wrangler or another CI system to deploy Pages, make sure both `public/` and `functions/` are included.

### 6. Test the deployment

After deployment, open your Pages URL in two different browsers or devices and test:

1. QR Code pairing.
2. Six-digit PIN pairing.
3. Nearby-device discovery on the same public IPv4 network.
4. A small file transfer.
5. A larger file transfer.
6. Reconnection and resume after a network interruption.
7. Camera scanning and file download on a mobile device.

Useful browser Console clues:

- `403`: Usually an origin check or deployment entry-point problem.
- `409`: The selected room has reached its connection limit.
- `429`: A connection, lookup, or signaling message limit was exceeded.
- `Refused to connect`: A `connect-src` or domain configuration issue in the CSP.
- `/api` returns `500`: The Pages `SIGNAL_ROOM` binding is missing or incorrectly named.

## Security Notes for a Public Repository

Do not commit any of the following:

```text
.env
.env.*
.dev.vars
.wrangler/
Cloudflare API tokens
R2 access keys or secret keys
Private keys or service-account files
```

The `.gitignore` file excludes common local environment and Wrangler files. You should still review the Git history before making the repository public to ensure that no secret was previously committed.

The production service URL does not need to be included in the frontend source. The frontend uses the same-origin `/api` path. Users who fork this project should deploy their own Worker, Durable Object, and Pages project.

## Limitations and Notes

- WebRTC file transfer runs directly between browsers. Without a TURN server, some restrictive network environments may not be able to establish a direct connection.
- Nearby IPv4 discovery uses the public IPv4 detected by the browser. If the probe services are unavailable, use QR Code or PIN pairing instead.
- Durable Object limits protect signaling connections only; they do not limit file data sent through the WebRTC `fileChannel`.
- Tailwind CSS is built locally during deployment. QRious and jsQR are still loaded from fixed-version external CDNs; they can be vendored locally later for stronger supply-chain control.
- Cloudflare plan quotas, Durable Object limits, and Pages settings may change. Refer to Cloudflare's official documentation when deploying.

## Local Checks

After changing HTML classes or CSS, regenerate and commit `public/tailwind.css`:

```bash
npm run build
```

After changing JavaScript, run:

```bash
node --input-type=module --check < public/app.js
node --input-type=module --check < public/scanner.js
node --input-type=module --check < public/webrtc.js
node --input-type=module --check < do-worker.js
```

## License

This project is licensed under the [MIT License](LICENSE).
