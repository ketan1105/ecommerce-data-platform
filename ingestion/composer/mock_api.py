from fastapi import FastAPI
from faker import Faker
import random
import uvicorn

fake = Faker("en_IN")
app  = FastAPI()

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Kitchen", "Sports"]

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/products")
def get_products(limit: int = 100):
    used_ids = random.sample(range(1000, 1100), min(limit, 100))
    products = []
    for prod_id in used_ids:
        products.append({
            "product_id"  : f"PROD-{prod_id}",
            "product_name": fake.bs().title(),
            "category"    : random.choice(CATEGORIES),
            "price"       : round(random.uniform(99, 9999), 2),
            "stock_qty"   : random.randint(0, 500),
            "supplier"    : fake.company(),
            "created_at"  : fake.date_this_year().isoformat(),
        })
    return {"data": products, "count": len(products)}

@app.get("/customers")
def get_customers(limit: int = 100):
    used_ids = random.sample(range(1000, 1100), min(limit, 100))
    customers = []
    for cust_id in used_ids:
        customers.append({
            "customer_id"  : f"CUST-{cust_id}",
            "name"         : fake.name(),
            "email"        : fake.email(),
            "phone"        : fake.phone_number(),
            "city"         : fake.city(),
            "state"        : fake.state(),
            "pincode"      : fake.postcode(),
            "registered_at": fake.date_this_decade().isoformat(),
            "tier"         : random.choice(["Bronze","Silver","Gold","Platinum"]),
        })
    return {"data": customers, "count": len(customers)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)