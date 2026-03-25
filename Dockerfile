# 使用 Python 官方轻量镜像
FROM python:3.11-slim

# 安装系统依赖（去掉 libgconf-2-4）
RUN apt-get update && apt-get install -y \
    wget unzip xvfb libxi6 libnss3 libxss1 libappindicator3-1 fonts-liberation \
    curl gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 安装 Google Chrome 稳定版
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && dpkg -i google-chrome-stable_current_amd64.deb || apt-get -f install -y \
    && rm google-chrome-stable_current_amd64.deb

# 安装匹配 Chrome 的 ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') \
    && wget https://chromedriver.storage.googleapis.com/$CHROME_VERSION/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip -d /usr/local/bin/ \
    && rm chromedriver_linux64.zip \
    && chmod +x /usr/local/bin/chromedriver

# 安装 Python 库
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

# 复制主程序
COPY main.py /app/main.py
WORKDIR /app

# 启动程序
CMD ["python", "main.py"]
