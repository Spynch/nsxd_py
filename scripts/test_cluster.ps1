$ErrorActionPreference = "Stop"

function Wait-ForHealth {
    param([string]$Url)
    for ($i = 0; $i -lt 20; $i++) {
        try {
            Invoke-RestMethod -Uri $Url -Method Get | Out-Null
            return
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    throw "Health check failed for $Url"
}

Docker compose up -d --build

Wait-ForHealth -Url "http://localhost:8001/health"
Wait-ForHealth -Url "http://localhost:8002/health"
Wait-ForHealth -Url "http://localhost:8003/health"

$leader = $null
foreach ($port in 8001, 8002, 8003) {
    $health = Invoke-RestMethod -Uri "http://localhost:$port/health" -Method Get
    if ($health.role -eq "leader") {
        $leader = $port
    }
}

if (-not $leader) {
    throw "Leader not found"
}

Write-Host "Leader is on port $leader"

Invoke-RestMethod -Uri "http://localhost:$leader/kv/put" -Method Post -ContentType "application/json" -Body '{"key":"demo","value":"value"}' | Out-Null

Write-Host "Stopping leader container"
$service = switch ($leader) {
    8001 { "node1" }
    8002 { "node2" }
    8003 { "node3" }
    default { throw "Unknown leader port" }
}
Docker compose stop $service | Out-Null

Start-Sleep -Seconds 5

$newLeader = $null
foreach ($port in 8001, 8002, 8003) {
    try {
        $health = Invoke-RestMethod -Uri "http://localhost:$port/health" -Method Get
        if ($health.role -eq "leader") {
            $newLeader = $port
        }
    } catch {
        # ignore stopped node
    }
}

if (-not $newLeader) {
    throw "New leader not found"
}

Write-Host "New leader is on port $newLeader"
Invoke-RestMethod -Uri "http://localhost:$newLeader/kv/get?key=demo" -Method Get | Out-Null

Docker compose down
