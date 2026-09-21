[CmdletBinding()]
param(
    [string]$ImageName = "metropulse-lambda:phase4-local"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) {
        throw $Message
    }
}

try {
    $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
    Push-Location $repoRoot
    try {
        $engine = docker info --format '{{.OSType}}/{{.Architecture}}'
        Assert-True ($LASTEXITCODE -eq 0) "Docker daemon is unavailable."
        Assert-True ($engine -in @("linux/x86_64", "linux/amd64")) "Expected linux/amd64 Docker engine; found $engine."

        Write-Output "Building $ImageName for linux/amd64..."
        $buildArguments = @(
            "buildx", "build",
            "--pull",
            "--platform", "linux/amd64",
            "--load",
            "--tag", $ImageName,
            "."
        )
        & docker @buildArguments
        $buildExitCode = $LASTEXITCODE
        Assert-True ($buildExitCode -eq 0) "Docker build failed with exit code $buildExitCode."

        $imageListArguments = @("image", "ls", "--quiet", "--no-trunc", $ImageName)
        $localImageId = & docker @imageListArguments
        $imageListExitCode = $LASTEXITCODE
        Assert-True ($imageListExitCode -eq 0) "Unable to query the local Docker image store."
        Assert-True (-not [string]::IsNullOrWhiteSpace(($localImageId -join ""))) "Build completed, but tagged image '$ImageName' is unavailable in the local Docker image store. Ensure the Buildx build uses --load."

        $inspectIdArguments = @("image", "inspect", "--format", "{{.Id}}", $ImageName)
        $imageId = & docker @inspectIdArguments
        Assert-True ($LASTEXITCODE -eq 0) "Unable to inspect image ID for '$ImageName'."

        $inspectSizeArguments = @("image", "inspect", "--format", "{{.Size}}", $ImageName)
        $imageSizeText = & docker @inspectSizeArguments
        Assert-True ($LASTEXITCODE -eq 0) "Unable to inspect image size for '$ImageName'."
        $imageSize = [int64]$imageSizeText

        $inspectCommandArguments = @("image", "inspect", "--format", "{{json .Config.Cmd}}", $ImageName)
        $configuredCommand = & docker @inspectCommandArguments
        Assert-True ($LASTEXITCODE -eq 0) "Unable to inspect image CMD for '$ImageName'."

        $inspectUserArguments = @("image", "inspect", "--format", "{{.Config.User}}", $ImageName)
        $configuredUser = & docker @inspectUserArguments
        Assert-True ($LASTEXITCODE -eq 0) "Unable to inspect image user for '$ImageName'."
        Assert-True ($configuredCommand -eq '["metropulse.aws.lambda_handler.lambda_handler"]') "Unexpected image CMD: $configuredCommand"
        Assert-True ([string]::IsNullOrEmpty($configuredUser)) "Image overrides Lambda's default runtime user: $configuredUser"

        $importArguments = @(
            "run", "--rm",
            "--platform", "linux/amd64",
            "--entrypoint", "python",
            $ImageName,
            "-c", "import pyarrow; from metropulse.aws.lambda_handler import lambda_handler as validation_handler; from metropulse.aws.compaction_lambda_handler import lambda_handler as compaction_handler; from metropulse.aws.audit_lambda_handler import lambda_handler as audit_handler; assert callable(validation_handler) and callable(compaction_handler) and callable(audit_handler); print('PYARROW_OK version=' + pyarrow.__version__); print('VALIDATION_HANDLER_IMPORT_OK'); print('COMPACTION_HANDLER_IMPORT_OK'); print('AUDIT_HANDLER_IMPORT_OK')"
        )
        $versions = & docker @importArguments
        Assert-True ($LASTEXITCODE -eq 0) "PyArrow or handler import failed."
        $versions | ForEach-Object { Write-Output $_ }

        $smokePath = (Resolve-Path "scripts/container_smoke.py").Path
        $smokeArguments = @(
            "run", "--rm",
            "--platform", "linux/amd64",
            "--env", "PYTHONPATH=/var/task",
            "--mount", "type=bind,source=$smokePath,target=/tmp/container_smoke.py,readonly",
            "--entrypoint", "python",
            $ImageName,
            "/tmp/container_smoke.py"
        )
        & docker @smokeArguments
        Assert-True ($LASTEXITCODE -eq 0) "Deterministic offline processing smoke test failed."

        $operationsSmokePath = (Resolve-Path "scripts/container_operations_smoke.py").Path
        $operationsSmokeArguments = @(
            "run", "--rm",
            "--platform", "linux/amd64",
            "--env", "PYTHONPATH=/var/task",
            "--mount", "type=bind,source=$operationsSmokePath,target=/tmp/container_operations_smoke.py,readonly",
            "--entrypoint", "python",
            $ImageName,
            "/tmp/container_operations_smoke.py"
        )
        & docker @operationsSmokeArguments
        Assert-True ($LASTEXITCODE -eq 0) "Deterministic offline compaction/audit smoke test failed."

        $auditPath = (Resolve-Path "scripts/audit_lambda_task.py").Path
        $auditArguments = @(
            "run", "--rm",
            "--platform", "linux/amd64",
            "--mount", "type=bind,source=$auditPath,target=/tmp/audit_lambda_task.py,readonly",
            "--entrypoint", "python",
            $ImageName,
            "/tmp/audit_lambda_task.py",
            "/var/task"
        )
        & docker @auditArguments
        Assert-True ($LASTEXITCODE -eq 0) "Container content audit failed."

        $historyArguments = @("history", "--no-trunc", $ImageName)
        $history = & docker @historyArguments
        Assert-True ($LASTEXITCODE -eq 0) "Docker history inspection failed."
        $secretPattern = '(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|SECRET_ACCESS_KEY|PASSWORD=|TOKEN=|PRIVATE_KEY)'
        Assert-True (-not ($history -match $secretPattern)) "Docker history contains a secret-bearing build argument or value."

        Write-Output "VALIDATION_HANDLER_CONFIG_OK metropulse.aws.lambda_handler.lambda_handler"
        Write-Output "COMPACTION_HANDLER_CONFIG_OK metropulse.aws.compaction_lambda_handler.lambda_handler"
        Write-Output "AUDIT_HANDLER_CONFIG_OK metropulse.aws.audit_lambda_handler.lambda_handler"
        Write-Output "LAMBDA_DEFAULT_USER_CONFIG_OK"
        Write-Output "HISTORY_SECRET_SCAN_OK"
        Write-Output "IMAGE_ID=$imageId"
        Write-Output "IMAGE_SIZE_BYTES=$imageSize"
        Write-Output "CONTAINER_VERIFICATION_OK"
    }
    finally {
        Pop-Location
    }
}
catch {
    Write-Error $_
    exit 1
}
