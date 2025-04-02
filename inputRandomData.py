import os
from pymongo import MongoClient 
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime, timedelta
import random
from bson.objectid import ObjectId

load_dotenv()
fake = Faker('es_ES')  # faker en españo

def connect_to_mongodb():
    try:
        uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB_NAME")
        
        client = MongoClient(uri)
        db = client[db_name]
        print("Conexión exitosa a MongoDB")
        return db
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        return None

def generate_random_users(num_users=100000):
    print("Generando datos aleatorios...")
    
    # Lista de productos posibles pero asgurar que haya bvarios prod1
    productos = ["Producto 1"] * 10 + [f"Producto {i}" for i in range(2, 20)]
    
    # Lista de tags posibles pero hay que asegurar varios "tag2"
    tags_list = ["tag2"] * 5 + [f"tag{i}" for i in range(1, 10) if i != 2]
    
    # Colores 
    colores = ["azul", "rojo", "verde", "amarillo", "negro", "blanco"]
    
    users = []
    
    for _ in range(num_users):

        nombre = fake.first_name()[:8] + " " + fake.last_name()[:8]  #limitar
        email = nombre.lower().replace(" ", ".")[:15] + "@" + fake.free_email_domain()

        # Generar historial de compras (entre 1 y 5compras)
        num_compras = random.randint(1, 5)
        historial_compras = []
        for _ in range(num_compras):
            producto = random.choice(productos)
            fecha_compra = fake.date_time_between(start_date="-2y", end_date="now").timestamp()
            historial_compras.append({
                "producto": producto,
                "fecha": fecha_compra
            })
        
        # Generar amigos 
        num_amigos = random.randint(0, 100)
        if random.random() < 0.01:  # 1% de usuarios con más de 1000 amigos
            num_amigos = random.randint(1000, 1100)
        amigos = [random.randint(1, 999999) for _ in range(num_amigos)]
        
        # Generar tags asegurar que algunos tengan "tag2"
        num_tags = random.randint(1, 3)
        user_tags = random.sample(tags_list, num_tags)
        if "tag2" not in user_tags and random.random() < 0.3:  # 30% de probabilidad de añadir tag2
            user_tags.append("tag2")
        
        # Crear el documento del usuario
        user = {
            "nombre": nombre,
            "email": email,
            "fecha_registro": fake.date_time_between(start_date="-5y", end_date="now"),
            "puntos": random.randint(0, 1000),
            "historial_compras": historial_compras,
            "direccion": {
                "calle": fake.street_name()[:10],
                "ciudad": fake.city(),
                "codigo_postal": int(fake.postcode()[:5])
            },
            "tags": user_tags,
            "activo": random.choice([True, False]),
            "notas": fake.word()[:15],
            "visitas": random.randint(0, 500),
            "amigos": amigos,
            "preferencias": {
                "color": random.choice(colores),
                "idioma": random.choice(["es", "en", "fr", "de"]),
                "tema": random.choice(["claro", "oscuro", "auto"])
            }
        }
        
        # Asegurar que algunos usuarios cumplan condiciones para las consultas
        if random.random() < 0.1:  # 10% de usuarios activos con >500 puntos
            user["activo"] = True
            user["puntos"] = random.randint(500, 1000)
        
        if random.random() < 0.1:  # 10% de usuarios con >100 visitas y tag2
            user["visitas"] = random.randint(100, 500)
            if "tag2" not in user["tags"]:
                user["tags"].append("tag2")
        
        if random.random() < 0.05:  # 5% de usuarios con color azul y entre 1000-2000 amigos
            user["preferencias"]["color"] = "azul"
            user["amigos"] = [random.randint(1, 999999) for _ in range(random.randint(1000, 2000))]
        
        users.append(user)
    
    return users

def insert_users(db, users, batch_size=1000):
    print(f"Insertando {len(users)} usuarios en la colección...")
    collection = db.usuarios
    
    # Insertar en lotes para mejor rendimiento
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        collection.insert_many(batch)
        print(f"Insertados {i + len(batch)}/{len(users)} documentos")
    
    print("Terminao")


print("Data generator")
db = connect_to_mongodb()

if db != None:
    # Generar 100,000 usuarios iniciales
    users = generate_random_users(20)
    insert_users(db, users)
    
    # pra quien haga el 1.6, aqui esta la funcion para agregar otros 50,000 usuarios adicionales 
    # additional_users = generate_random_users(50000)
    # insert_users(db, additional_users)

