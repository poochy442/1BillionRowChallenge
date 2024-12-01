$pythonScriptPath = "python/calculateAverage.py"
$jsonFilePath = "key.json"

$commandResult = Measure-Command {
    $pythonOutput = python $pythonScriptPath | Out-String
}
$executionTime = $commandResult.TotalSeconds

$stationsData = @{}
$pythonOutput.Split("`n") | ForEach-Object {
    $line = $_.Trim()
    if ($line) {
        $fields = $line -split ";"
        if ($fields.Length -eq 4) {
            $stationName = $fields[0]
            $mean = [float]$fields[2]
            $stationsData[$stationName] = $mean
        }
    }
}

$keyData = Get-Content $jsonFilePath | ConvertFrom-Json

$allCorrect = $true
foreach ($stationName in $stationsData.Keys) {
    $calculatedMean = $stationsData[$stationName]
    $correctMean = $keyData.$stationName

    if ($calculatedMean -ne $correctMean) {
        Write-Host "Error: $stationName - Calculated Mean: $calculatedMean, Expected Mean: $correctMean"
        $allCorrect = $false
    }
}

if ($allCorrect) {
    Write-Host "Success: All stations have correct means."
    Write-Host "Execution Time: $executionTime seconds"
} else {
    Write-Host "Verification failed: Some stations have incorrect means."
}
