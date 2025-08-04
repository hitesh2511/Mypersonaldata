// Save this file as your_puppeteer_script.js in your repository root

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const INDEXES = {
  "NIFTY 50": "NIFTY%2050",
  "NIFTY NEXT 50": "NIFTY%20NEXT%2050",
  "NIFTY MIDCAP 150": "NIFTY%20MIDCAP%20150",
  "NIFTY MIDCAP 50": "NIFTY%20MIDCAP%2050",
  "NIFTY MIDCAP 100": "NIFTY%20MIDCAP%20100",
  "NIFTY TOTAL MARKET": "NIFTY%20TOTAL%20MARKET"
};

(async () => {
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
  const page = await browser.newPage();

  // Open NSE main market data page to initialize cookies and JS environment
  await page.goto('https://www.nseindia.com/market-data/live-equity-market', { waitUntil: 'networkidle2' });

  const combinedData = [];

  for (const [name, code] of Object.entries(INDEXES)) {
    try {
      const url = `https://www.nseindia.com/api/equity-stockIndices?index=${code}`;

      // Fetch the JSON data using page.evaluate to keep cookies and headers intact
      const response = await page.evaluate(async (url) => {
        const res = await fetch(url, {
          headers: {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://www.nseindia.com/market-data/live-equity-market',
            'User-Agent': navigator.userAgent,
          },
          credentials: 'include'
        });
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        return await res.json();
      }, url);

      if (!response || !response.data || response.data.length === 0) {
        console.warn(`⚠️ No data returned for ${name}`);
        continue;
      }

      for (const item of response.data) {
        item.Index = name;
        combinedData.push(item);
      }

      console.log(`✅ Fetched data for ${name}`);
      await new Promise(r => setTimeout(r, 2000)); // polite delay between requests

    } catch (error) {
      console.error(`❌ Error fetching ${name}:`, error.message);
    }
  }

  if (combinedData.length === 0) {
    console.error("❌ No data fetched for any index. Exiting.");
    await browser.close();
    process.exit(1);
  }

  // Convert combinedData to CSV format
  const headers = Object.keys(combinedData[0]);
  const csvRows = [
    headers.join(','), // header row
    ...combinedData.map(row => headers.map(field => {
      let value = row[field] ?? '';
      value = value.toString().replace(/"/g, '""'); // escape quotes
      return `"${value}"`;
    }).join(','))
  ];

  const csvContent = csvRows.join('\n');

  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir);
  }

  const outputFile = path.join(dataDir, 'all_indices.csv');
  fs.writeFileSync(outputFile, csvContent);
  console.log(`✅ Combined CSV saved to ${outputFile}`);

  await browser.close();
})();
