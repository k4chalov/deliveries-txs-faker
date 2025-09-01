from faker import Faker
fake = Faker()

# fake.name()
# # 'Lucy Cechtelar'

# fake.address()
# # '426 Jordy Lodge
# #  Cartwrightshire, SC 88120-6700'

# fake.text()

for _ in range(10):
  print(fake.name())