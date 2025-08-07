from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import date

@dataclass
class Transaction:
    """
    A dataclass to represent a single transaction with all its columns.
    This enforces a consistent structure for all transaction data.
    """
    TransactionFingerprint: str
    Amount: float
    Account: str
    Date: date
    Description: str
    Institution: str
    Category: str
    MyCategory: str

class SpendingClassifier:

    def __init__(self):
        """
        We use a hierachy tree to represent the different kinds of categories we can have.
        The top levels are income, spending, and transfers.

        Income:
        Within income, we have different categories to separate out our real salary vs random cash we get.
        So for example, we will distinguish a deposit into our account from a cash back credit.
        That being said, peer-to-peer payments will not be included as income.
        It could be positive, but it may instead be mapped as a "credit" in one of the spending credits.

        (When we apply our rules, we will do text based filtering before we look at the amounts to account for this._

        Spending:
        Within spending, we have lots of different categories.
        "Needs" are rent, utilities, things that I can't not spend money on.
        "Shoulds" are things that aren't "needed" but I really should spend on. Health/fitness, seeing parents, etc.
        "Wants" are just wants. Things that I am doing selfishly to spend money.
        I do this because other apps didn't have this middle ground between needs/wants that I wanted to capture.
        I want to make sure I cut from wants before I cut from shoulds.

        Transfers:
        This is what you expect.

        Design Choices:
        It may be better to introduce an arbitrary tagging system, but that sounds kinda hard.
        Let's start with this first and then address limitations as they arise.
        """
        self.categories = {
            "income": {
                "gift": {}, # Parents sending money
                "salary": {}, # Money from job
                "atm": {}, # Deposit into ATM
                "bank_fees": {}, # dividends, interests
                "refunds": {} # from stores
            },
            "spending": {
                "atm": {}, # Withdrawal from ATM
                "needs": {
                    "rent": {}, # just rent
                    "utilities": {}, # coned, wifi, etc
                    "home": {}, # toilet paper, cleaning, etc
                    "health": {}, # skin care, dental, etc
                    "loans": {}, # student loans
                },
                "shoulds": {
                    "grocery": {}, # incentivize cooking more
                    "fitness": {}, # recurring gym, nyrr. nice to haves go in hobbies
                    "services": {}, # iCloud, spotify, loseit, etc
                    "commuting": {}, # Subway, amtrak to go home
                },
                "wants": {
                    "dining": {
                        "treats": {}, # dessert, coffee, snacks
                        "dates": {}, # food with partner
                        "friends": {}, # meals with friends
                        "solo": {}, # food just for myself
                    },
                    "shopping": {
                        "clothes": {},
                        "hobbies": {},
                        "gift": {},
                    },
                    "entertainment": {
                        "alcohol": {}, # bars, pocha
                        "shows": {}, # raves, concerts, etc
                        "sober_fun": {}, # maybe art cafe or something
                    },
                    "travel": {
                        "lodging": {}, # Hotel cost
                        "transportation": {}, # Airfare, car rental
                        "food": {}, # It's ok to have a dedicated food tracker, so it doesn't overlap
                        "activities": {}, # Tours, etc
                        "shopping": {}, # Souveniers
                    }
                },
            },
            "transfers": {
                "credit_card_payments": {}, # just paying bills
                "stocks": {}, # Transferred for FZROX
                "long_term_cash": {}, # HYSA, TBills, SGOV, CD
                "large_venmos": {}, # Large venmo payments that go via bank
            },
        }

    def _collapse_categories(self, categories_dict, prefix=""):
        collapsed_list = []

        for key, value in categories_dict.items():
            # Create the path for the current key
            if prefix:
                current_path = f"{prefix}.{key}"
            else:
                current_path = key

            # Check if the value is a dictionary
            if isinstance(value, dict) and value:
                # If it's a non-empty dictionary, recurse
                collapsed_list.extend(self._collapse_categories(value, current_path))
            else:
                # If it's an empty dictionary (or not a dictionary), it's a final category
                collapsed_list.append(current_path)

        return collapsed_list

    def _parse_row_to_transaction(self, row):
        return Transaction(
            TransactionFingerprint=row.get("TransactionFingerprint"),
            Amount=row.get("Amount"),
            Account=row.get("Account"),
            Date=row.get("Date"),
            Description=row.get("Description"),
            Institution=row.get("Institution"),
            Category=row.get("Category"),
            MyCategory="uncategorized")

    def classify(self, df):
        print(self._collapse_categories(self.categories))
        df.apply(self._parse_row_to_transaction, axis=1)
