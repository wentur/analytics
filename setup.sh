#!/bin/bash
# goai.rest — Setup script
# Run once after cloning: bash setup.sh

echo "Setting up goai.rest..."

# Create .env from template
if [ ! -f .env ]; then
    cp env .env
    echo "✅ Created .env"
else
    echo "⏭  .env already exists"
fi

# Create .gitignore
if [ ! -f .gitignore ]; then
    cp gitignore .gitignore
    echo "✅ Created .gitignore"
else
    echo "⏭  .gitignore already exists"
fi

# Create .streamlit config
mkdir -p .streamlit
if [ ! -f .streamlit/config.toml ]; then
    cp streamlit_config.toml .streamlit/config.toml
    echo "✅ Created .streamlit/config.toml"
else
    echo "⏭  .streamlit/config.toml already exists"
fi

echo ""
echo "Done! Now run:"
echo "  pip install -r requirements.txt"
echo "  streamlit run dashboard.py"
