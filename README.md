# Step 1 — install	Python + aiohttp + SQLite
Open you terminal

Open Git Bash

cd backend	
pip install -r requirements.txt	
	
# Step 2 — start server	
Start server by Git Bash: uvicorn api:app --reload --port 8000
Ctrl + C in terminal to stop the server
	
# Step 3 — open frontend	
Open frontend/index.html in your browser
(Use Live server in Visual Studio Code or you can deploy it on netlify but remember to start your local machine as server everytime you want to perform a crawl. Local machine crawls are free)

Crawl the website you want by providing URL input
Download the Report in CSV or XLSX format
