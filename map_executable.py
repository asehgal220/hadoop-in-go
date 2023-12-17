import sys
from abc import ABC, abstractmethod
import re
import argparse

NOFILE="NOFILE"

class MapleJob(ABC):
    def __init__(self, input_file: str) -> None:
        self.input_file=input_file
    
    @abstractmethod
    def process_file(self):
        pass

class UnitMapleJob(MapleJob):
    def __init__(self, input_file: str) -> None:
        super().__init__(input_file)

    def process_file(self):
        try:
            # Open the input file for reading

            with open(self.input_file, 'r') as file:
                # Read and print each line
                for line in file:
                    line = line.strip()
                    line = line.split(',')
                    key, val = line[0].strip(), '_'.join(line[1:]).strip()
                    sys.stdout.write('[' + key + ': ' + val + ']\n')
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()

class FilterMapleJob(MapleJob):
    def __init__(self, input_file: str, line_regex: str) -> None:
        self.line_regex = f'({line_regex})'
        super().__init__(input_file)

    def process_file(self):
        try:
            # Open the input file for reading
            with open(self.input_file, 'r') as file:
                # Read and print each line
                for line in file:
                    line = line.strip()
                    res = re.findall(self.line_regex, line)
                    if len(res) > 0:
                        line = line.split(',')
                        key, val = line[0].strip(), ','.join(line[1:]).strip()
                        sys.stdout.write('[' + key + ': ' + val + ']\n')
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()

class JoinM1MapleJob(MapleJob):
    def __init__(self, input_file: str, column_name: str) -> None:
        self.column_name = column_name
        super().__init__(input_file)

    def process_file(self):
        try:
            columnIdx = -1
            firstLine = True
            # Open the input file for reading
            with open(self.input_file, 'r') as file:
                # Read and print each line
                for line in file:
                    line = line.strip()
                    lineValues = line.split(',')
                    if firstLine:
                        firstLine = False
                        for i, val in enumerate(lineValues):
                            if val == self.column_name:
                                columnIdx = i
                                break
                        continue
                    
                    key, val = lineValues[columnIdx].strip(), ','.join(lineValues[:columnIdx]+lineValues[columnIdx+1:]).strip()
                    sys.stdout.write('[' + key + ': ' + val + ']\n')
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()


class JoinR1JuiceJob(MapleJob):
    def __init__(self, input_file: str) -> None:
        super().__init__(input_file)

    def process_file(self):
        try:
            aggregation = {}
            # Open the input file for reading
            with open(self.input_file, 'r') as file:
                # Read and print each line
                for line in file:
                    line = line.strip()
                    parsed = line.split(':')
                    key = parsed[0][1:]
                    value = parsed[1][1:-1]
                    if key in aggregation:
                        aggregation[key].append(value)
                    else:
                        aggregation[key] = [value]
                    
                for key, value in aggregation:
                    output = ""
                    for v in value:
                        output += v
                        output += ','
                    sys.stdout.write('[' + key + ': ' + output[:-1] + ']\n')
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()
    
class CompositionMapleJob(MapleJob):
    def __init__(self, input_file: str, line_regex: str) -> None:
        self.line_regex = line_regex
        super().__init__(input_file)

    def process_file(self):
        try:
            firstLine = True
            columnIdx = -1
            # Open the input file for reading
            with open(self.input_file, 'r') as file:
                # Read and print each line
                for line in file:
                    line = line.strip()
                    line = line.split(',')
                    if firstLine:
                        firstLine = False
                        for i, columnNames in enumerate(line):
                            if columnNames == "Interconne":
                                columnIdx = i
                                break
                        continue
                    if line[columnIdx] == self.line_regex:
                        if line[columnIdx-1] == " ":
                            line[columnIdx-1] = ""
                        sys.stdout.write('[' + line[columnIdx] + ': ' + line[columnIdx-1] + ']\n')
                    
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()

class CompositionJuiceJob(MapleJob): # Should only be 1 reduce job
    def __init__(self, input_file: str) -> None:
        super().__init__(input_file)

    def process_file(self):
        try:
            aggregation = {}
            # Open the input file for reading
            with open(self.input_file, 'r') as file:
                # Read and print each line
                totalNumElements = 0
                for line in file:
                    line = line.strip()
                    parsed = line.split(':')
                    key = parsed[0][1:]
                    value = parsed[1][1:-1]
                    if value in aggregation:
                        aggregation[value] += 1
                    else:
                        aggregation[value] = 1
                    totalNumElements += 1
                for key, value in aggregation.items():
                    sys.stdout.write('[' + key + ': ' + str(value/totalNumElements) + ']\n')
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()
# class JoinMapleJob(MapleJob):
#     def __init__(self, input_file: str, line_regex: str) -> None:
#         self.line_regex = f'({line_regex})'
#         super().__init__(input_file)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--inputfile", type=str, default="NOFILE")
    parser.add_argument("-p", "--pattern", type=str, default="Video,Radio")

    return parser.parse_args()

if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided

    # Get the input file from the command-line arguments
    args = parse_args()
    

    input_file = args.inputfile
    
    if input_file == NOFILE:
        print("Usage: ./maplexec -f input_file -p <pattern>")
        sys.exit(1)

    # maple=UnitMapleJob(input_file)
    regex = args.pattern
    # maple=CompositionMapleJob(input_file, regex)
    maple=CompositionJuiceJob(input_file)


    # Process the file
    maple.process_file()
