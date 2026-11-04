import pandas as pd, numpy as np, os

os.makedirs("datos", exist_ok=True)
for i in range(50):
    df = pd.DataFrame({
        "ciudad": np.random.choice(["Bogota", "Medellin", "Cali", "Riohacha"], 200_000),
        "producto": np.random.choice(["A", "B", "C"], 200_000),
        "ventas": np.random.randint(1, 1000, 200_000),
        })
    df.to_csv(f"datos/ventas_{i}.csv", index=False)
    print("Creado", i)
