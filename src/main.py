from classify import SpendingClassifier
from csv_reader import read_transactions

'''
First, write a giant file. Only once you have working stuff should you start to break it down.
'''

def main():
    df = read_transactions("../data/transactions_*.csv", show_duplicates=True)
    sc = SpendingClassifier()
    sc.classify(df)

if __name__ == "__main__":
    main()