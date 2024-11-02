## Objective
The objective of this project is to analyze sales data for an e-commerce company. We use a CSV file containing the previous year's sales data, organized with the following columns:
- transaction_id: Unique identifier for each transaction
- client_id: ID of the client who made the purchase
- product_id: ID of the product sold
- date: Date of the transaction
- amount: Transaction amount
- category: Product category

1. Run the Project with Docker Compose :

```
docker-compose up -d
```

2. Access the Spark Master Container :

In your terminal run this command :
```
docker exec -it spark-master bash
```

3. Navigate to the application folder and run it :

```
cd
spark-submit /app/app.py
```