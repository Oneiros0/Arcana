import json

items = []
input_file = open('../dynamic/7_27_2019_dynamic.json')
json_array = json.load(input_file)

# with open("data_file.json", "w") as write_file:
#     json.dump(data, write_file)

for item in json_array:
    items.append(item)

print(items)