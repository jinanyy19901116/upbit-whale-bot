#!/bin/bash
# 一键部署 Arkham Selenium 监控脚本（Linux）

set -e
echo "更新系统..."
sudo apt update && sudo apt upgrade -y

echo "安装依赖库..."
sudo apt install -y wget unzip xvfb libxi6 libgconf-2-4 libnss3 libxss1 libappindicator3-1 fonts-liberation python3 python3-pip

echo "安装 Python 库..."
pip3 install --upgrade pip
pip3 install selenium requests

echo "安装 Google Chrome..."
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb || sudo apt-get -f install -y
rm google-chrome-stable_current_amd64.deb

CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+')
echo "Chrome 版本: $CHROME_VERSION"

echo "安装匹配的 ChromeDriver..."
wget https://chromedriver.storage.googleapis.com/$CHROME_VERSION/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/local/bin/
sudo chmod +x /usr/local/bin/
rm chromedriver_linux64.zip

echo "运行 main.py..."
python3 /app/main.py
