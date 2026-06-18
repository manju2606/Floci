# Retail Store Database Port-Forwards
# Run this script to expose retail store in-cluster databases to localhost
# Ports used (avoid conflicts with Floci's own databases):
#   catalog-mysql      -> localhost:33306
#   cart-dynamodb      -> localhost:38000
#   checkout-redis     -> localhost:36379
#   orders-postgresql  -> localhost:35432
#   orders-rabbitmq    -> localhost:45672  (management UI)

Write-Host "Starting retail store database port-forwards..." -ForegroundColor Cyan
Write-Host ""

$forwards = @(
    @{ Name="catalog-mysql";      Svc="catalog-mysql";      Local=33306; Remote=3306  },
    @{ Name="cart-dynamodb";      Svc="cart-carts-dynamodb"; Local=38000; Remote=8000 },
    @{ Name="checkout-redis";     Svc="checkout-redis";      Local=36379; Remote=6379 },
    @{ Name="orders-postgresql";  Svc="orders-postgresql";   Local=35432; Remote=5432 },
    @{ Name="orders-rabbitmq-ui"; Svc="orders-rabbitmq";     Local=45672; Remote=15672}
)

$jobs = @()
foreach ($f in $forwards) {
    Write-Host "  $($f.Name): localhost:$($f.Local) -> $($f.Svc):$($f.Remote)" -ForegroundColor Green
    $job = Start-Job -ScriptBlock {
        param($svc, $local, $remote)
        kubectl port-forward "svc/$svc" "${local}:${remote}" -n default 2>&1
    } -ArgumentList $f.Svc, $f.Local, $f.Remote
    $jobs += $job
}

Write-Host ""
Write-Host "All port-forwards running. Press Ctrl+C to stop." -ForegroundColor Yellow
Write-Host ""
Write-Host "Connection info:" -ForegroundColor Cyan
Write-Host "  Catalog MySQL     : mysql -h 127.0.0.1 -P 33306 -u mydbadmin -pkalyandb101 catalog"
Write-Host "  Cart DynamoDB     : aws dynamodb list-tables --endpoint-url http://localhost:38000"
Write-Host "  Checkout Redis    : redis-cli -h localhost -p 36379"
Write-Host "  Orders PostgreSQL : psql -h localhost -p 35432 -U mydbadmin -d orders"
Write-Host "  Orders RabbitMQ   : http://localhost:45672  (guest / guest)"
Write-Host ""

try {
    while ($true) {
        Start-Sleep -Seconds 5
        foreach ($job in $jobs) {
            if ($job.State -eq 'Failed') {
                Write-Host "  Job $($job.Id) failed — restarting..." -ForegroundColor Red
            }
        }
    }
} finally {
    $jobs | Stop-Job
    $jobs | Remove-Job
    Write-Host "Port-forwards stopped." -ForegroundColor Yellow
}
