import subprocess

def main():
	proc = subprocess.Popen(['hdfs', 'dfs', '-copyFromLocal', '/home/stephen/Training/Metanauts_BD/"Task 4"/sneaker-database.json', 'hdfs:///ten_times'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)

main()
