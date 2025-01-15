#!/bin/bash

# Update system packages
echo "Updating system packages..."
sudo apt update

# Install ODBC development libraries
echo "Installing ODBC development libraries..."
sudo apt install -y curl unixodbc-dev

sudo apt install -y unixodbc-dev

# Optional: Install Microsoft ODBC driver for SQL Server
echo "Installing Microsoft ODBC driver for SQL Server (if needed)..."
sudo apt install -y msodbcsql17

# Verify libodbc installation
echo "Verifying libodbc installation..."
if ldconfig -p | grep -q libodbc; then
    echo "libodbc is installed."
else
    echo "libodbc installation failed. Please check your system."
    exit 1
fi


# Reinstall pyodbc in the Python environment
echo "Reinstalling pyodbc in the Python environment..."
pip uninstall -y pyodbc
pip install pyodbc


echo "Setup complete."