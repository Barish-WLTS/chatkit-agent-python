#!/bin/bash

# Multi-Brand Chatbot System - Quick Setup Script
# This script automates the setup process

set -e  # Exit on error

echo "=========================================="
echo "Multi-Brand Chatbot System Setup"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check if MySQL is installed
echo "Checking MySQL installation..."
if command -v mysql &> /dev/null; then
    print_success "MySQL is installed"
else
    print_error "MySQL is not installed"
    echo ""
    echo "Please install MySQL first:"
    echo "  Ubuntu/Debian: sudo apt install mysql-server"
    echo "  macOS: brew install mysql"
    echo "  Windows: Download from https://dev.mysql.com/downloads/installer/"
    exit 1
fi

# Check if Python is installed
echo "Checking Python installation..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    print_success "Python $PYTHON_VERSION is installed"
else
    print_error "Python 3 is not installed"
    exit 1
fi

# Get database credentials
echo ""
echo "=========================================="
echo "Database Configuration"
echo "=========================================="
echo ""

read -p "MySQL root password: " -s MYSQL_ROOT_PASSWORD
echo ""

read -p "New database name [chatbot_system]: " DB_NAME
DB_NAME=${DB_NAME:-chatbot_system}

read -p "New database user [chatbot_user]: " DB_USER
DB_USER=${DB_USER:-chatbot_user}

read -p "New database user password: " -s DB_PASSWORD
echo ""

# Create database and user
echo ""
print_info "Creating database and user..."

mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<EOF
CREATE DATABASE IF NOT EXISTS $DB_NAME CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '$DB_USER'@'localhost' IDENTIFIED BY '$DB_PASSWORD';
GRANT ALL PRIVILEGES ON $DB_NAME.* TO '$DB_USER'@'localhost';
FLUSH PRIVILEGES;
EOF

if [ $? -eq 0 ]; then
    print_success "Database created successfully"
else
    print_error "Failed to create database"
    exit 1
fi

# Run schema SQL if file exists
if [ -f "schema.sql" ]; then
    print_info "Running database schema..."
    mysql -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < schema.sql
    print_success "Database schema created"
else
    print_error "schema.sql not found. Please create it from the SQL schema artifact."
fi

# Get SMTP configuration
echo ""
echo "=========================================="
echo "SMTP Configuration"
echo "=========================================="
echo ""

read -p "SMTP Host [mail.gbpseo.in]: " SMTP_HOST
SMTP_HOST=${SMTP_HOST:-mail.gbpseo.in}

read -p "SMTP Port [465]: " SMTP_PORT
SMTP_PORT=${SMTP_PORT:-465}

read -p "SMTP Username [chatbot@gbpseo.in]: " SMTP_USERNAME
SMTP_USERNAME=${SMTP_USERNAME:-chatbot@gbpseo.in}

read -p "SMTP Password: " -s SMTP_PASSWORD
echo ""

# Create .env file
echo ""
print_info "Creating .env file..."

cat > .env <<EOF
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
DB_NAME=$DB_NAME

# SMTP Configuration
SMTP_HOST=$SMTP_HOST
SMTP_PORT=$SMTP_PORT
SMTP_USERNAME=$SMTP_USERNAME
SMTP_PASSWORD=$SMTP_PASSWORD
SMTP_FROM_EMAIL=$SMTP_USERNAME

# Server Configuration
PORT=3000
EOF

print_success ".env file created"

# Install Python dependencies
echo ""
print_info "Installing Python dependencies..."

if [ -f "requirements.txt" ]; then
    pip3 install -r requirements.txt
    print_success "Dependencies installed"
else
    print_error "requirements.txt not found"
fi

# Create necessary directories
echo ""
print_info "Creating necessary directories..."
mkdir -p templates
mkdir -p imgs
print_success "Directories created"

# Test database connection
echo ""
print_info "Testing database connection..."

python3 -c "
import aiomysql
import asyncio
import os

async def test_connection():
    try:
        pool = await aiomysql.create_pool(
            host='localhost',
            port=3306,
            user='$DB_USER',
            password='$DB_PASSWORD',
            db='$DB_NAME'
        )
        pool.close()
        await pool.wait_closed()
        print('✓ Database connection successful')
        return True
    except Exception as e:
        print(f'✗ Database connection failed: {e}')
        return False

asyncio.run(test_connection())
"

# Final instructions
echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
print_success "All setup steps completed successfully"
echo ""
echo "Next steps:"
echo "  1. Ensure all Python files are in place:"
echo "     - main.py (updated version)"
echo "     - database.py (new file)"
echo "     - agents.py (your existing file)"
echo ""
echo "  2. Ensure templates/chatbot.html exists"
echo ""
echo "  3. Start the application:"
echo "     python3 main.py"
echo ""
echo "  4. Access the chatbot at:"
echo "     http://localhost:3000"
echo ""
echo "=========================================="
echo ""