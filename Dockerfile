FROM python:3.11-slim

# ================= 系统依赖 =================
RUN apt-get update && apt-get install -y \
    wget unzip xvfb libxi6 libnss3 libxss1 fonts-liberation \
    curl gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ================= 安装 Chrome =================
RUN mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
    | gpg --dearmor -o /etc/apt/keyrings/google.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# ================= 自动匹配 ChromeDriver =================
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') \
    && echo "Chrome version: $CHROME_VERSION" \
    && DRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION") \
    && echo "Driver version: $DRIVER_VERSION" \
    && wget https://chromedriver.storage.googleapis.com/$DRIVER_VERSION/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip -d /usr/local/bin/ \
    && rm chromedriver_linux64.zip \
    && chmod +x /usr/local/bin/chromedriver

# ================= Python依赖 =================
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

# ================= 代码 =================
COPY main.py /app/main.py
WORKDIR /app

CMD ["python", "main.py"]
