# Proxy Setup Guide for India Electricians Scraper

## Why Use Proxies?

Websites like JustDial, IndiaMART, and Sulekha have anti-bot measures:
- IP rate limiting
- CAPTCHA challenges
- Browser fingerprinting
- Request pattern detection

Proxies help by:
- Rotating IP addresses
- Distributing requests across multiple IPs
- Avoiding rate limits and blocks

---

## Option 1: Free Proxies (Testing Only)

**Not recommended for production** - slow, unreliable, often blocked.

```bash
# Enable free proxies in .env
USE_FREE_PROXIES=true
FREE_PROXY_COUNT=20
```

Or add proxies manually to `proxies.txt`:
```
# Get free proxies from:
# https://free-proxy-list.net/
# https://www.sslproxies.org/

103.152.112.162:80
47.243.177.45:8088
```

---

## Option 2: BrightData (Recommended)

**Best for production** - Rotating residential IPs, high success rate.

### Setup:
1. Sign up at https://brightdata.com/
2. Create a "Residential" zone
3. Get your credentials

### Configure .env:
```env
BRIGHTDATA_CUSTOMER_ID=brd-customer-hl_12345678
BRIGHTDATA_ZONE=residential
BRIGHTDATA_PASSWORD=your_password_here
```

### Pricing:
- Pay-as-you-go: ~$15/GB
- Subscription: ~$500/month for 40GB
- Free trial available

---

## Option 3: ScraperAPI (Easy Setup)

**Good for beginners** - Simple API, handles CAPTCHAs.

### Setup:
1. Sign up at https://www.scraperapi.com/
2. Get your API key

### Configure .env:
```env
SCRAPERAPI_KEY=your_api_key_here
```

### Pricing:
- Free tier: 5,000 requests/month
- Hobby: $49/month for 100,000 requests
- Business: $149/month for 500,000 requests

---

## Option 4: Oxylabs

**Premium option** - High-quality residential proxies.

### Setup:
1. Sign up at https://oxylabs.io/
2. Get your credentials

### Configure .env:
```env
OXYLABS_USERNAME=customer-your_username
OXYLABS_PASSWORD=your_password
```

### Pricing:
- Residential: ~$15/GB
- Datacenter: ~$2/GB

---

## Option 5: WebShare.io (Budget Friendly)

**Good balance** - Affordable rotating proxies.

### Setup:
1. Sign up at https://www.webshare.io/
2. Get your proxy list

### Add to proxies.txt:
```
# WebShare proxies (format: ip:port:user:pass)
185.199.228.220:7300:user:pass
185.199.231.45:8382:user:pass
```

### Pricing:
- Free tier: 10 proxies
- Paid: Starting at $5.49/month for 100 proxies

---

## Option 6: Your Own Proxy List

Add any proxies to `proxies.txt`:

```
# Format: host:port
192.168.1.1:8080

# Format: host:port:username:password
proxy.example.com:3128:myuser:mypass

# Format: URL
http://user:pass@proxy.example.com:8080
```

---

## Running the Scraper

### 1. Configure your proxies
Edit `.env` file or `proxies.txt`

### 2. Run with proxies
```bash
cd india_electricians_scraper
python scrape_with_proxy.py
```

### 3. Monitor progress
The scraper will:
- Test proxies before scraping
- Rotate proxies automatically
- Retry failed requests with different proxies
- Mark bad proxies and remove them

---

## Tips for Best Results

1. **Use residential proxies** - They're less likely to be blocked
2. **Increase delays** - Set `REQUEST_DELAY_MIN=5` and `REQUEST_DELAY_MAX=10`
3. **Scrape during off-peak hours** - Less chance of detection
4. **Use multiple proxy providers** - Distribute load across different IP ranges
5. **Monitor success rates** - If below 50%, proxies may be burned

---

## Troubleshooting

### "All proxies have failed"
- Get fresh proxies
- Try a different provider
- Increase delay between requests

### "403 Forbidden" errors
- Website detected bot activity
- Rotate User-Agents (already implemented)
- Try different proxy provider
- Wait 24 hours before retrying

### Slow scraping
- Normal with proxies + delays
- Consider running overnight
- Use more proxies for parallel scraping

---

## Cost Estimate

For scraping 200 cities across India:
- ~10,000 - 50,000 requests
- BrightData: ~$50-100
- ScraperAPI: ~$49-149/month
- Free proxies: $0 (but may not work)

---

## Quick Start

```bash
# 1. Copy example env
cp .env.example .env

# 2. Add your ScraperAPI key (easiest option)
echo "SCRAPERAPI_KEY=your_key_here" >> .env

# 3. Run the scraper
python scrape_with_proxy.py
```
