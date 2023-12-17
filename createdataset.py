import csv
import argparse
import random
import string

def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def random_job():
    jobs = ["Doctor", "Engineer", "Teacher", "Nurse", "Scientist", "Pilot", "Astronaut", "Lawyer", "Zookeeper", "Baker"]
    return random.choice(jobs)

def random_age():
    return random.randint(18, 100)

def random_affinity():
    affinities = ["Stark", "Lannister", "Targaryen", "Baratheon", "Greyjoy", "Martell", "Tyrell", "Arryn", "Frey", "Bolton"]
    return random.choice(affinities)

def generate_csv(n, ofilename):
    with open(ofilename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "age", "affinity", "job"])
        for i in range(n):
            writer.writerow([i+1, random_string(10), random_age(), random_affinity(), random_job()])

def main():
    parser = argparse.ArgumentParser(description="Generate a CSV file with random data.")
    parser.add_argument("n", type=int, help="The number of rows to generate.")
    parser.add_argument("--ofilename", type=str, default="data.csv", help="The name of the output file. Default is data.csv.")
    args = parser.parse_args()
    generate_csv(args.n, args.ofilename)

if __name__ == "__main__":
    main()
