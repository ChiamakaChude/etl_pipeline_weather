<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body>
    <h1>Real-Time Weather and Traffic Data Integration Project</h1>
    <p>
        This project integrates real-time weather data from <strong>OpenWeather</strong> and traffic data from <strong>TomTom</strong> 
        to provide a comprehensive overview of city conditions. The data pipeline is designed to be efficient, scalable, and maintainable, using the following key features:
    </p>
    <h2>Features</h2>
    <ul>
        <li>
            <strong>Data Sources:</strong>
            <ul>
                <li><strong>Weather Data:</strong> Fetched from OpenWeather API.</li>
                <li><strong>Traffic Data:</strong> Fetched from TomTom Traffic API.</li>
            </ul>
        </li>
        <li>
            <strong>ETL Pipeline:</strong> Extracts, transforms, and loads data into a database (Microsoft SQL Server).
        </li>
        <li>
            <strong>Orchestration:</strong> Airflow is used for scheduling and managing data workflows.
        </li>
        <li>
            <strong>Technologies Used:</strong>
            <ul>
                <li>Python for data processing.</li>
                <li>REST API integrations.</li>
                <li>Pandas for data manipulation.</li>
              <li>Microsoft SQL Server for the database.</li>
              <li>Apache Airflow for orchectration.</li>
            </ul>
        </li>
        <li>
            <strong>Core Functionalities:</strong>
            <ul>
                <li>Automates data retrieval from multiple APIs.</li>
                <li>Merges weather and traffic data for each city.</li>
                <li>Provides up-to-date insights for analysis or downstream applications.</li>
            </ul>
        </li>
    </ul>
    <p>
        This project demonstrates real-time data integration and orchestration, showcasing skills in API integration, data engineering, and workflow automation.
    </p>
</body>
</html>
