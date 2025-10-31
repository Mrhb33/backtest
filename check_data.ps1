# Check ClickHouse data for 2025 BTCUSDT
$headers = @{Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("backtest:backtest123"))}

# Check if data exists
Write-Host "Checking if data exists in ClickHouse..."
$query1 = "SELECT count() as total_bars, min(open_time_ms) as first_bar, max(open_time_ms) as last_bar FROM backtest.data WHERE symbol='BTCUSDT' AND interval='1m'"
$response1 = Invoke-WebRequest -Uri "http://localhost:18123/?query=$([System.Web.HttpUtility]::UrlEncode($query1))" -Headers $headers
Write-Host "Data count response: $($response1.Content)"

# Check price ranges for each month
Write-Host "`nChecking price ranges for each month..."
$query2 = @"
SELECT 
    toYYYYMM(toDateTime(open_time_ms/1000)) as month,
    min(close) as min_price,
    max(close) as max_price,
    count() as bars
FROM backtest.data 
WHERE symbol='BTCUSDT' AND interval='1m' 
GROUP BY month 
ORDER BY month
"@
$response2 = Invoke-WebRequest -Uri "http://localhost:18123/?query=$([System.Web.HttpUtility]::UrlEncode($query2))" -Headers $headers
Write-Host "Price ranges by month:"
Write-Host $response2.Content

# Check 5m data specifically for July-September
Write-Host "`nChecking 5m data for July-September 2025..."
$query3 = @"
SELECT 
    toDate(toDateTime(open_time_ms/1000)) as date,
    min(close) as min_price,
    max(close) as max_price,
    count() as bars
FROM backtest.data 
WHERE symbol='BTCUSDT' 
    AND interval='5m' 
    AND toDateTime(open_time_ms/1000) >= '2025-07-01 00:00:00'
    AND toDateTime(open_time_ms/1000) <= '2025-09-30 23:59:59'
GROUP BY date 
ORDER BY date
LIMIT 10
"@
$response3 = Invoke-WebRequest -Uri "http://localhost:18123/?query=$([System.Web.HttpUtility]::UrlEncode($query3))" -Headers $headers
Write-Host "5m data for July-September (first 10 days):"
Write-Host $response3.Content
