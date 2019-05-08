import csv

def save_csv(data, path):
    with open(path, 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for row in data:
            new_row = list()
            for item in row:
                if isinstance(item, str):
                    item = unicode(item, "utf-8")
                if isinstance(item, unicode):
                    item = item.encode("utf-8")
                new_row.append(item)
            writer.writerow(new_row)



def load_csv(path):
    with open(path, 'rb') as csvfile:
        csvfile.seek(0)
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader, None)
        for row in reader:
            yield row
