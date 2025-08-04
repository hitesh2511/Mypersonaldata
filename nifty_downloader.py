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
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();

  await page.goto('https://www.nseindia.com/market-data/live-equity-market', {waitUntil: 'networkidle2'});

  const combinedData = [];

  for (const [name, code] of Object.entries(INDEXES)) {
    const url = `https://www.nseindia.com/api/equity-stockIndices?index=${code}`;
    const response = await page.evaluate((url) =>
      fetch(url, {
        headers: {
          'Accept': 'application/json, text/javascript, */*; q=0.01',
          'Referer': 'https://www.nseindia.com/market-data/live-equity-market',
          'User-Agent': navigator.userAgent
        }
      }).then(res => res.json()), url);

    if (!response.data) {
      console.log(`⚠️ No data for ${name}`);
      continue;
    }

    response.data.forEach(item => {
      item.Index = name;
      combinedData.push(item);
    });

    console.log(`Fetched ${name}`);
  }

  // Convert combined data to CSV
  const csvHeaders = Object.keys(combinedData[0]);
  const csvRows = [
    csvHeaders.join(','),
    ...combinedData.map(row => csvHeaders.map(field => `"${(row[field] ?? '').toString().replace(/"/g, '""')}"`).join(','))
  ].join('\n');

  // Save CSV file
  const outputPath = path.join('data', 'all_indices.csv');
  fs.mkdirSync('data', { recursive: true });
  fs.writeFileSync(outputPath, csvRows);

  console.log(`✅ Combined data saved as ${outputPath}`);

  await browser.close();
})();
