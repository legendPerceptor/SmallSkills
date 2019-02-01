## Use Polipo to make your shell under a proxy

### Installation

```
brew install polipo
```

### Configuration

write the following config to $.poliporc$ in your home path.

```"
socksParentProxy = "127.0.0.1:1086"
socksProxyType = socks5
proxyAddress = "127.0.0.1"

proxyPort = 8123
```

### Lauch polipo when logged in

```bash
ln -sfy /usr/local/opt/polipo/*.plist ~/Library/Launch/LaunchAgents/
```

### Launch and Stop

```
launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.polipo.plist
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.polipo.plist
```

### Test  the proxy

```
set http_proxy "http://localhost:8123"
curl ip.sb
```







