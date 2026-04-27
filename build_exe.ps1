python -m pip install -r requirements.txt
python -m PyInstaller `
  --noconfirm `
  --onefile `
  --windowed `
  --name "WB_Supply_Calculator" `
  wb_supply_calculator.py

Write-Host ""
Write-Host "Готово. EXE находится в папке .\dist\WB_Supply_Calculator.exe"
