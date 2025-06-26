import pandas as pd
from typing import Callable, Dict, Any
from enum import Enum


class Category(Enum):
    """
    Predefined categories for transaction classification.
    Each category has a string value that matches its name in title case.
    """

    GROCERIES = "Groceries"
    RESTAURANTS = "Restaurants"
    SALARY = "Salary"
    ELECTRICITY = "Electricity"
    SAVINGS = "Savings"
    RIDESHARE = "Rideshare"
    UNCATEGORIZED = "Uncategorized"

    @classmethod
    def get_all_categories(cls):
        """Return a list of all category values."""
        return [category.value for category in cls]

    @classmethod
    def from_string(cls, category_str: str):
        """Convert a string to a Category enum member if it exists."""
        try:
            return cls(category_str)
        except ValueError:
            return cls.UNCATEGORIZED


class Rule:
    """
    Represents a single classification rule.
    """

    def __init__(
        self,
        description: str,
        condition: Callable[[Dict[str, Any]], bool],
        category: Category,
    ):
        """
        Initialize a rule with a description, condition function, and category.

        Args:
            description: Human-readable description of the rule
            condition: Function that takes a transaction dict and returns a boolean
            category: The Category enum value to assign if condition is True
        """
        self.description = description
        self.condition = condition
        self.category = category

    def apply_condition(self, item: Dict[str, Any]) -> bool:
        """
        Apply the rule's condition to an item.

        Args:
            item: Dictionary containing transaction data

        Returns:
            bool: True if the condition is met, False otherwise
        """
        try:
            return self.condition(item)
        except (KeyError, TypeError, AttributeError) as e:
            # Log or handle the error if needed
            return False

    def get_category(self, item: Dict[str, Any]) -> Category:
        """
        Get the category for the given item if the rule matches.

        Args:
            item: Dictionary containing transaction data

        Returns:
            Category: The category if the rule matches, None otherwise
        """
        if self.apply_condition(item):
            return self.category
        return None


class RuleEngine:
    """
    Applies a set of rules to classify transactions.
    Rules are applied in order, and the first matching rule assigns a category.
    """

    def is_category_equal(self, item, candidate):
        return item["Category"] == candidate

    def description_has(
        self,
        item: Dict[str, Any],
        substr: str,
    ) -> bool:
        if not item or not isinstance(item, dict):
            return False

        description = item.get("Description")
        if not description or not isinstance(description, str):
            return False

        if not substr:
            return False

        description = description.lower()
        substr = substr.lower()
        return substr in description

    def __init__(self):
        self.rules = [
            Rule(
                description="Keep groceries the same",
                condition=lambda item: self.is_category_equal(item, "Groceries"),
                category=Category.GROCERIES,
            ),
            Rule(
                description="Keep dining the same",
                condition=lambda item: self.is_category_equal(
                    item, "Restaurants/Dining"
                ),
                category=Category.RESTAURANTS,
            ),
            Rule(
                description="Keep utilities the same",
                condition=lambda item: self.is_category_equal(
                    item, "Energy, Gas & Electric"
                ),
                category=Category.ELECTRICITY,
            ),
            Rule(
                description="Keep salary the same",
                condition=lambda item: self.is_category_equal(item, "Paycheck/Salary"),
                category=Category.SALARY,
            ),
            Rule(
                description="Keep rideshare the same",
                condition=lambda item: self.is_category_equal(item, "Rideshare"),
                category=Category.RIDESHARE,
            ),
            Rule(
                description="Ally HYSA",
                condition=lambda item: self.is_category_equal(item, "Transfers") and self.description_has("ALLY BANK DES"),
                category=Category.SAVINGS,
            ),
            Rule(
                description="Fallback: Uncategorized transactions",
                condition=lambda _: True,  # Always matches
                category=Category.UNCATEGORIZED,
            ),
        ]

    def classify_item(self, item: Dict[str, Any]) -> Category:
        """
        Classify a single transaction item.

        Args:
            item: Dictionary containing transaction data

        Returns:
            Category: The assigned category
        """
        for rule in self.rules:
            category = rule.get_category(item)
            if category is not None:
                return category

        return Category.UNCATEGORIZED

    def classify_dataframe(
        self, df: pd.DataFrame, output_column: str = "Smarter Category"
    ) -> pd.DataFrame:
        """
        Classify all transactions in a DataFrame.

        Args:
            df: Input DataFrame containing transaction data
            output_column: Name of the column to store the category

        Returns:
            DataFrame: Input DataFrame with an additional column for the category
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame.")

        df[output_column] = df.apply(
            lambda row: self.classify_item(row.to_dict()).value, axis=1
        )
        return df


###############################################################################
# CLI / quick demo when executed as a script
###############################################################################

if __name__ == "__main__":
    data = {
        "Account": ["Checking", "Venmo", "Checking", "Checking"],
        "Category": [
            "Groceries",
            "Online Payment",
            "Fast Food & Convenience",
            "Salary",
        ],
        "Description": [
            "TRADER JOES #123",
            "ELECTRICITY FROM ROOMMATE",
            "CHIPOTLE ORDER",
            "PAYCHECK ABC CORP",
        ],
        "Institution": ["Bank", "Venmo", "Bank", "Bank"],
        "Amount": [-45.0, 30.0, -12.50, 1500.0],
    }
    df_mock = pd.DataFrame(data)
    df_mock["Amount"] = pd.to_numeric(df_mock["Amount"])
    df_mock["Description"] = df_mock["Description"].fillna("")
    df_mock["Category"] = df_mock["Category"].fillna("")
    df_mock["Account"] = df_mock["Account"].fillna("")

    # Instantiate the RuleEngine with the list of Rule objects
    engine = RuleEngine()

    # Classify the DataFrame
    classified_df = engine.classify_dataframe(df_mock.copy())

    print("--- Classified DataFrame (with Rule Class) ---")
    print(classified_df[["Description", "Amount", "Smarter Category"]])
