#!/bin/bash
LATEST_VERSION=$(curl -s https://msedgedriver.azureedge.net/LATEST_RELEASE_STABLE)
echo "Installing MSEdgeDriver $LATEST_VERSION..."

wget "https://msedgedriver.azureedge.net/$LATEST_VERSION/edgedriver_linux64.zip" -O /tmp/edgedriver.zip
unzip -o /tmp/edgedriver.zip -d /tmp/
sudo mv /tmp/msedgedriver /usr/local/bin/
sudo chmod +x /usr/local/bin/msedgedriver

echo "Installed: $(msedgedriver --version)"