from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from kafka import KafkaProducer
import threading
import time
import json

# Path to msedgedriver.exe
DRIVER_PATH = "msedgedriver.exe"

# List of stock symbols
STOCKS = ["AAPL", "MSFT", "TSLA", "ORCL", "AMZN", "NFLX"]

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'stock-prices'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# Function to scrape a single stock
def scrape_stock(symbol):
    options = webdriver.EdgeOptions()
    #options.add_argument("--headless")  # comment out to see the browser
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    
    service = Service(executable_path=DRIVER_PATH)
    driver = webdriver.Edge(service=service, options=options)
    
    url = f"https://finance.yahoo.com/chart/{symbol}"
    driver.get(url)
    time.sleep(10)  # wait for chart to load completely
    
    last_price = None
    while True:
        try:
            price_element = driver.find_element(By.XPATH, "//fin-streamer[@data-field='regularMarketPrice']")
            price = price_element.text.strip()
            
            if price and price != last_price:
                last_price = price
                data = {
                    "symbol": symbol,
                    "price": float(price.replace(',', '')),
                    "timestamp": time.time()
                }
                print(f"{symbol} New Price: {data}")
                
                # Send data to Kafka
                try:
                    producer.send(KAFKA_TOPIC, key=symbol, value=data)
                    producer.flush()
                    print(f"✓ Sent to Kafka: {symbol} - ${data['price']}")
                except Exception as kafka_error:
                    print(f"✗ Kafka Error for {symbol}: {kafka_error}")
            
            time.sleep(3)
        except Exception as e:
            print(f"{symbol} Scraping Error:", e)
            time.sleep(5)
    
    driver.quit()


if __name__ == "__main__":
    threads = []
    
    print("Starting stock price scraper with Kafka producer...")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Tracking stocks: {', '.join(STOCKS)}")
    print("-" * 60)
    
    for symbol in STOCKS:
        t = threading.Thread(target=scrape_stock, args=(symbol,))
        t.daemon = True
        t.start()
        threads.append(t)
        time.sleep(2)  # Stagger thread starts
    
    # Keep main thread alive
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        producer.close()
        print("Kafka producer closed.")