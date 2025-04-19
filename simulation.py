import time
import uuid # Used for generating unique IDs

class Order:
    """Represents a single order in the market."""

    def __init__(self, symbol: str, side: str, order_type: str, quantity: int, price: float = None, order_id: str = None, timestamp: float = None):
        """
        Initializes an Order object.

        Args:
            symbol (str): The ticker symbol of the instrument (e.g., 'STOCK_A').
            side (str): 'BUY' or 'SELL'.
            order_type (str): 'LIMIT' or 'MARKET'.
            quantity (int): The number of units to trade. Must be positive.
            price (float, optional): The limit price for LIMIT orders. Should be None for MARKET orders. Defaults to None.
            order_id (str, optional): A unique ID for the order. If None, a new UUID is generated. Defaults to None.
            timestamp (float, optional): The time the order was created (as a Unix timestamp). If None, the current time is used. Defaults to None.

        Raises:
            ValueError: If input arguments are invalid (e.g., unknown side/type, negative quantity).
        """

        # --- Validate Inputs ---
        if side not in ('BUY', 'SELL'):
            raise ValueError(f"Invalid side: {side}. Must be 'BUY' or 'SELL'.")
        if order_type not in ('LIMIT', 'MARKET'):
            raise ValueError(f"Invalid order_type: {order_type}. Must be 'LIMIT' or 'MARKET'.")
        if order_type == 'LIMIT' and (price is None or price <= 0):
            raise ValueError("LIMIT orders require a positive price.")
        if order_type == 'MARKET' and price is not None:
            print(f"Warning: Price ({price}) provided for MARKET order will be ignored.")
            price = None # Market orders don't have a fixed price
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValueError(f"Quantity must be a positive integer, got {quantity}.")
        if not isinstance(symbol, str) or not symbol:
             raise ValueError("Symbol must be a non-empty string.")

        # --- Assign Properties ---
        self.symbol = symbol
        self.side = side # 'BUY' or 'SELL'
        self.order_type = order_type # 'LIMIT' or 'MARKET'
        self.quantity = quantity # Original requested quantity
        self.price = price # None for MARKET orders

        # Use provided ID/timestamp or generate new ones
        self.order_id = order_id if order_id is not None else str(uuid.uuid4())
        self.timestamp = timestamp if timestamp is not None else time.time()

        # Properties that might change later
        self.filled_quantity = 0 # How much of this order has been filled
        self.status = 'OPEN' # Potential statuses: OPEN, PARTIALLY_FILLED, FILLED, CANCELLED

    @property
    def remaining_quantity(self) -> int:
        """Calculates the quantity yet to be filled."""
        return self.quantity - self.filled_quantity

    @property
    def is_filled(self) -> bool:
        """Checks if the order is fully filled."""
        return self.remaining_quantity <= 0

    def fill(self, quantity_filled: int):
        """Marks a portion (or all) of the order as filled."""
        if quantity_filled <= 0:
            return # No change
        if quantity_filled > self.remaining_quantity:
            # Should not happen with correct matching logic, but good to check
            print(f"Warning: Attempted to fill {quantity_filled} but only {self.remaining_quantity} remaining for order {self.order_id}. Filling remaining.")
            quantity_filled = self.remaining_quantity

        self.filled_quantity += quantity_filled

        if self.is_filled:
            self.status = 'FILLED'
        elif self.filled_quantity > 0:
            self.status = 'PARTIALLY_FILLED'
        # else status remains 'OPEN' if filled_quantity was <= 0

    def cancel(self):
        """Marks the order as cancelled if it's not already fully filled."""
        if not self.is_filled and self.status != 'CANCELLED':
            self.status = 'CANCELLED'
            # print(f"Order {self.order_id} cancelled.") # Optional logging
        else:
             print(f"Warning: Cannot cancel order {self.order_id} with status {self.status}.")

    def __repr__(self) -> str:
        """Provides a useful string representation for debugging."""
        price_str = f"@{self.price}" if self.order_type == 'LIMIT' else '(MARKET)'
        return (f"Order(ID={self.order_id[-6:]}, Symbol={self.symbol}, Side={self.side}, Type={self.order_type}, "
                f"Qty={self.quantity}, Price={price_str}, Filled={self.filled_quantity}, Status={self.status})")

# --- Example Usage (for testing purposes) ---
if __name__ == "__main__":
    print("Creating some sample orders...")

    try:
        buy_limit_order = Order(symbol='STOCK_A', side='BUY', order_type='LIMIT', price=100.50, quantity=10)
        print(buy_limit_order)

        sell_market_order = Order(symbol='STOCK_B', side='SELL', order_type='MARKET', quantity=50)
        print(sell_market_order)

        buy_market_order_with_price = Order(symbol='STOCK_A', side='BUY', order_type='MARKET', price=99.0, quantity=5) # Price will be ignored
        print(buy_market_order_with_price)

        # Example of filling an order
        print(f"\nFilling part of {buy_limit_order.order_id[-6:]}...")
        buy_limit_order.fill(3)
        print(buy_limit_order)
        print(f"Remaining quantity: {buy_limit_order.remaining_quantity}")

        print(f"\nFilling the rest of {buy_limit_order.order_id[-6:]}...")
        buy_limit_order.fill(7)
        print(buy_limit_order)
        print(f"Is filled? {buy_limit_order.is_filled}")

        print(f"\nAttempting to cancel filled order {buy_limit_order.order_id[-6:]}...")
        buy_limit_order.cancel()

        print(f"\nAttempting to cancel open order {sell_market_order.order_id[-6:]}...")
        sell_market_order.cancel()
        print(sell_market_order)


        # Example of invalid order
        print("\nTrying to create an invalid order...")
        invalid_order = Order(symbol='STOCK_C', side='HOLD', order_type='LIMIT', price=10, quantity=5)

    except ValueError as e:
        print(f"\nCaught expected error: {e}")