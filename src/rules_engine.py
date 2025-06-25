import pandas as pd


class Rule:
    """
    Represents a single classification rule.
    """

    def __init__(self, description, condition, my_category):
        if not isinstance(description, str):
            raise TypeError("Rule description must be a string.")
        if not callable(condition):
            raise TypeError("Rule condition must be a callable (function or lambda).")
        # my_category can be str or callable, no type check here for flexibility

        self.description = description
        self.condition = condition
        self.my_category = my_category

    def apply_condition(self, item):
        """Applies the rule's condition to an item."""
        try:
            return self.condition(item)
        except KeyError as e:
            # Handle cases where the condition tries to access a non-existent key.
            # This rule's condition doesn't apply, so treat as false for this item.
            # You might log this for debugging.
            # print(f"Warning: Rule '{self.description}' condition failed for item due to missing key: {e}")
            return False

    def get_category(self, item):
        """Determines the category for an item based on the rule's my_category."""
        if callable(self.my_category):
            return self.my_category(item)
        else:
            return self.my_category


class RuleEngine:
    """
    A generic rule engine for classifying items based on a predefined set of rules.
    Rules are applied sequentially. The first matching rule assigns a category.
    """

    def __init__(self):
        self.rules = [
            Rule(
                description="Spending: Groceries (Trader Joes example)",
                condition=lambda item: item["Amount"] < 0
                and "TRADER JOES" in str(item["Description"]).upper(),
                my_category="Groceries",
            ),
            Rule(
                description="Spending: Dining Out - Quick Bites (Chipotle example)",
                condition=lambda item: item["Amount"] < 0
                and "CHIPOTLE" in str(item["Description"]).upper(),
                my_category="Dining Out - Quick Bites",
            ),
            Rule(
                description="Income: Expense Reimbursements (Electricity from Venmo)",
                condition=lambda item: item["Amount"] > 0
                and "ELECTRICITY" in str(item["Description"]).upper()
                and "VENMO" in str(item["Account"]).upper(),
                my_category="Expense Reimbursements",
            ),
            Rule(
                description="Fallback: Uncategorized transactions",
                condition=lambda item: True,  # This rule always matches if reached
                my_category=lambda item: f"Uncategorized {'Spending' if item['Amount'] < 0 else 'Income'}",
            ),
        ]

    def classify_item(self, item):
        for rule_obj in self.rules:
            if rule_obj.apply_condition(item):
                return rule_obj.get_category(item)
        return "Uncategorized"

    def classify_dataframe(
        self, df, output_column="Smarter Category", default_category="Uncategorized"
    ):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                "Input must be a Pandas DataFrame for classify_dataframe method."
            )

        df[output_column] = default_category

        for rule_obj in self.rules:  # Iterate over Rule objects now
            unclassified_rows_indicator = df[output_column] == default_category

            # Apply the condition method of the Rule object
            rows_matching_current_rule = df.loc[unclassified_rows_indicator].apply(
                rule_obj.apply_condition, axis=1
            )

            rows_to_update = unclassified_rows_indicator & rows_matching_current_rule

            if callable(
                rule_obj.my_category
            ):  # Check my_category directly from Rule object
                # Apply the get_category method of the Rule object
                df.loc[rows_to_update, output_column] = df.loc[rows_to_update].apply(
                    rule_obj.get_category, axis=1
                )
            else:
                df.loc[rows_to_update, output_column] = rule_obj.my_category

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
