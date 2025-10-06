# Set execution policy
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Function to start the backend server
function Start-Backend {
    Write-Host "Starting Backend Server..." -ForegroundColor Green
    cd C:\Users\SYRIN0011\Documents\bq2dbx-migrator
    & C:/Users/SYRIN0011/AppData/Local/pypoetry/Cache/virtualenvs/bq2dbx-migrator-y8aE9Zf0-py3.13/Scripts/Activate.ps1
    python -m uvicorn app:app --reload
}

# Function to start the frontend server
function Start-Frontend {
    Write-Host "Starting Frontend Server..." -ForegroundColor Green
    cd C:\Users\SYRIN0011\Documents\bq2dbx-migrator\react\bq2dbx-ui
    npm start
}

# Start both servers in separate windows
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Start-Backend"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Start-Frontend"