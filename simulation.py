import time
import uuid # Used for generating unique IDs
import heapq # Provides efficient heap queue algorithm (useful for priority queues)

# ========================================
#       Order Class Definition
# ========================================
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
        # Removed the warning here as remove_order might call cancel on an already cancelled order, which is okay.
        # else:
        #      print(f"Warning: Cannot cancel order {self.order_id} with status {self.status}.")

    def __repr__(self) -> str:
        """Provides a useful string representation for debugging."""
        price_str = f"@{self.price:.2f}" if self.order_type == 'LIMIT' and self.price is not None else '(MARKET)'
        # Shorten ID for readability
        short_id = self.order_id.split('-')[0] # Show only first part of UUID
        return (f"Order(ID={short_id}, Symbol={self.symbol}, Side={self.side}, Type={self.order_type}, "
                f"Qty={self.quantity}, Price={price_str}, Filled={self.filled_quantity}, Status={self.status})")


# ========================================
#      OrderBook Class Definition
# ========================================
class OrderBook:
    """Represents the order book for a single instrument."""

    def __init__(self, symbol: str):
        """
        Initializes an OrderBook for a specific symbol.

        Args:
            symbol (str): The ticker symbol this order book is for (e.g., 'STOCK_A').
        """
        if not isinstance(symbol, str) or not symbol:
             raise ValueError("OrderBook symbol must be a non-empty string.")
        self.symbol = symbol
        # Bids: Max-heap (negated prices) storing tuples: (-price, timestamp, order)
        # Highest price has highest priority (lowest negated value).
        self.bids = []
        # Asks: Min-heap storing tuples: (price, timestamp, order)
        # Lowest price has highest priority.
        self.asks = []
        # Keep track of orders by ID for quick cancellation/lookup
        self._orders_map = {} # {order_id: order_object}

    def add_order(self, order: Order):
        """Adds a valid LIMIT order to the order book."""
        if not isinstance(order, Order):
            raise TypeError("Can only add Order objects to the order book.")
        if order.symbol != self.symbol:
            raise ValueError(f"Order symbol '{order.symbol}' does not match OrderBook symbol '{self.symbol}'.")
        if order.order_type != 'LIMIT':
            # For now, this basic book only holds LIMIT orders. Market orders are handled by matching engine immediately.
            print(f"Info: MARKET order {order.order_id} not added to book (handled by matching engine).")
            return None # Indicate not added
        if order.status not in ('OPEN', 'PARTIALLY_FILLED'):
             print(f"Warning: Order {order.order_id} has status {order.status} and will not be added.")
             return None # Indicate not added
        if order.order_id in self._orders_map:
             print(f"Warning: Order {order.order_id} already exists in the book.")
             # Ensure the existing order object in the map is the one we're dealing with
             # This can happen if an order object was created but add_order failed/returned early before
             if self._orders_map[order.order_id] is order:
                 return order
             else:
                 # This case is less likely but guards against weird state
                 print(f"Error: Different order object with same ID {order.order_id} detected.")
                 return None


        print(f"Adding order to {self.symbol} book: {order}")
        self._orders_map[order.order_id] = order

        # Use heapq for efficient retrieval of best bid/ask
        # heapq implements min-heaps. For bids (max price priority), store negated prices.
        entry = (order.price, order.timestamp, order) # Standard tuple for asks
        if order.side == 'BUY':
            # Negate price for max-heap behavior using heapq (smaller negative number is higher price)
             heapq.heappush(self.bids, (-order.price, order.timestamp, order))
        elif order.side == 'SELL':
             heapq.heappush(self.asks, entry)

        return order # Return the added order


    def remove_order(self, order_id: str):
        """Removes an order from the order book map using its ID (e.g., due to cancellation or fill) and marks it cancelled."""
        if order_id not in self._orders_map:
            print(f"Warning: Order ID {order_id} not found in the book map for removal.")
            return None

        order_to_remove = self._orders_map.pop(order_id) # Remove from map
        print(f"Removing order reference from {self.symbol} book map: {order_to_remove}")

        # Mark the order object itself as cancelled. This is the crucial step.
        # The heap items containing this order will be ignored later by get/pop methods.
        order_to_remove.cancel()

        # Note: The order technically remains in the heap structure(s) but will be ignored
        # when encountered during get/pop because its status is now 'CANCELLED'.

        return order_to_remove


    def get_order(self, order_id: str) -> Order | None:
         """Retrieves an order from the book's map by its ID without removing it."""
         return self._orders_map.get(order_id, None)

    # --- Getters for Best Bid/Ask (without removing) ---
    # These need to ignore cancelled/filled orders at the top of the heap

    def _clean_heap_top(self, heap):
         """Internal helper to remove invalid orders from the top of a heap."""
         while heap and heap[0][2].status not in ('OPEN', 'PARTIALLY_FILLED'):
              heapq.heappop(heap) # Remove cancelled/filled order from top

    def get_best_bid(self) -> float | None:
        """Returns the highest bid price, ignoring invalid orders at the top."""
        self._clean_heap_top(self.bids) # Ensure top is valid
        return -self.bids[0][0] if self.bids else None # Negate back to get actual price

    def get_best_ask(self) -> float | None:
        """Returns the lowest ask price, ignoring invalid orders at the top."""
        self._clean_heap_top(self.asks) # Ensure top is valid
        return self.asks[0][0] if self.asks else None

    # --- Pop Best Bid/Ask Order (for matching engine use later) ---
    # These retrieve AND remove the best order, ignoring invalid ones.

    def pop_best_bid_order(self) -> Order | None:
        """Retrieves and removes the best bid order, ignoring invalid orders."""
        self._clean_heap_top(self.bids) # Ensure top is valid
        if not self.bids:
            return None
        # Pop the best valid order from heap
        _, _, best_bid_order = heapq.heappop(self.bids)
        # Also remove it from map (if it somehow still existed - pop should be main removal point for matching)
        if best_bid_order.order_id in self._orders_map:
             del self._orders_map[best_bid_order.order_id]
        return best_bid_order


    def pop_best_ask_order(self) -> Order | None:
        """Retrieves and removes the best ask order, ignoring invalid orders."""
        self._clean_heap_top(self.asks) # Ensure top is valid
        if not self.asks:
            return None
        # Pop the best valid order from heap
        _, _, best_ask_order = heapq.heappop(self.asks)
        # Also remove it from map
        if best_ask_order.order_id in self._orders_map:
             del self._orders_map[best_ask_order.order_id]
        return best_ask_order

    def __repr__(self) -> str:
        """Provides a basic string representation of the order book state."""
        # Make copies to avoid modifying heaps during inspection
        temp_bids = list(self.bids)
        temp_asks = list(self.asks)

        # Clean tops for accurate best bid/ask display
        while temp_bids and temp_bids[0][2].status not in ('OPEN', 'PARTIALLY_FILLED'):
             heapq.heappop(temp_bids)
        while temp_asks and temp_asks[0][2].status not in ('OPEN', 'PARTIALLY_FILLED'):
             heapq.heappop(temp_asks)

        best_bid_price = -temp_bids[0][0] if temp_bids else None
        best_ask_price = temp_asks[0][0] if temp_asks else None

        # Count valid orders efficiently using the map which should be accurate
        valid_bid_count = sum(1 for order in self._orders_map.values() if order.side == 'BUY' and order.status in ('OPEN', 'PARTIALLY_FILLED'))
        valid_ask_count = sum(1 for order in self._orders_map.values() if order.side == 'SELL' and order.status in ('OPEN', 'PARTIALLY_FILLED'))

        # Format prices nicely
        best_bid_str = f"{best_bid_price:.2f}" if best_bid_price is not None else "None"
        best_ask_str = f"{best_ask_price:.2f}" if best_ask_price is not None else "None"


        return (f"OrderBook({self.symbol}): "
                f"{valid_bid_count} Bids (Best: {best_bid_str}), "
                f"{valid_ask_count} Asks (Best: {best_ask_str}), "
                f"MapSize: {len(self._orders_map)}")


    def display(self, level_limit=5):
        """Prints a formatted view of the top levels of the order book."""
        print(f"\n--- Order Book for {self.symbol} ---")

        # Aggregate quantities by price level directly from the map (more reliable than iterating heaps)
        ask_levels = {} # {price: {'total_qty': 0, 'order_count': 0}}
        bid_levels = {} # {price: {'total_qty': 0, 'order_count': 0}}

        for order in self._orders_map.values():
             if order.status not in ('OPEN', 'PARTIALLY_FILLED'):
                 continue # Skip inactive orders

             price = order.price
             qty = order.remaining_quantity
             levels = ask_levels if order.side == 'SELL' else bid_levels

             if price not in levels:
                 levels[price] = {'total_qty': 0, 'order_count': 0}
             levels[price]['total_qty'] += qty
             levels[price]['order_count'] += 1


        # Display Asks (sorted low to high price)
        print("Asks (Price - Qty - Orders):")
        ask_prices_sorted = sorted(ask_levels.keys())
        if not ask_prices_sorted:
            print("  <empty>")
        else:
            displayed_count = 0
            for price in ask_prices_sorted:
                if displayed_count >= level_limit: break
                level_data = ask_levels[price]
                print(f"  {price:.2f} - {level_data['total_qty']} ({level_data['order_count']})")
                displayed_count += 1

        print("-" * 20) # Separator

        # Display Bids (sorted high to low price)
        print("Bids (Price - Qty - Orders):")
        bid_prices_sorted = sorted(bid_levels.keys(), reverse=True)
        if not bid_prices_sorted:
             print("  <empty>")
        else:
            displayed_count = 0
            for price in bid_prices_sorted:
                 if displayed_count >= level_limit: break
                 level_data = bid_levels[price]
                 print(f"  {price:.2f} - {level_data['total_qty']} ({level_data['order_count']})")
                 displayed_count += 1

        print("--- End Book ---\n")

# ========================================
#      Test Block
# ========================================
if __name__ == "__main__":
    print("--- Testing Order Class (Basic) ---")
    # Keep one simple order test if needed
    try:
        test_order = Order(symbol='TEST', side='BUY', order_type='LIMIT', price=50.0, quantity=100)
        print(f"Created: {test_order}")
        test_order.fill(20)
        print(f"Filled 20: {test_order}")
        print(f"Remaining: {test_order.remaining_quantity}")
        test_order.cancel() # Test cancelling a partially filled order
        print(f"Cancelled: {test_order}")
    except ValueError as e:
        print(f"Error creating test order: {e}")

    print("\n--- Testing OrderBook Class ---")
    book_a = OrderBook(symbol='STOCK_A')
    print(f"Initial book: {book_a}")
    book_a.display()

    # Create some orders
    o1 = Order(symbol='STOCK_A', side='BUY', order_type='LIMIT', price=99.0, quantity=10)
    time.sleep(0.01) # Ensure slightly different timestamps for time priority demo
    o2 = Order(symbol='STOCK_A', side='SELL', order_type='LIMIT', price=101.0, quantity=5)
    time.sleep(0.01)
    o3 = Order(symbol='STOCK_A', side='BUY', order_type='LIMIT', price=99.5, quantity=20)
    time.sleep(0.01)
    o4 = Order(symbol='STOCK_A', side='SELL', order_type='LIMIT', price=101.0, quantity=15) # Same price as o2
    time.sleep(0.01)
    o5 = Order(symbol='STOCK_A', side='BUY', order_type='LIMIT', price=99.0, quantity=7) # Same price as o1
    time.sleep(0.01)
    o6 = Order(symbol='STOCK_A', side='SELL', order_type='MARKET', quantity=10) # Market order (won't be added to book)
    time.sleep(0.01)
    o7 = Order(symbol='STOCK_C', side='BUY', order_type='LIMIT', price=10.0, quantity=10) # Wrong symbol
    time.sleep(0.01)
    o8 = Order(symbol='STOCK_A', side='BUY', order_type='LIMIT', price=98.0, quantity=30) # Another bid


    # Add orders to the book
    print("\nAdding orders...")
    book_a.add_order(o1)
    book_a.add_order(o2)
    book_a.add_order(o3)
    book_a.add_order(o4)
    book_a.add_order(o5)
    book_a.add_order(o6) # Should print info msg and not add to book
    try:
        book_a.add_order(o7) # Should raise ValueError
    except ValueError as e:
        print(f"Caught expected error adding o7: {e}")
    book_a.add_order(o8)


    print(f"\nBook state after adds: {book_a}")
    book_a.display()

    # Test retrieval and best bid/ask
    print(f"Get Best Bid Price: {book_a.get_best_bid()}") # Should be 99.50 (from o3)
    print(f"Get Best Ask Price: {book_a.get_best_ask()}") # Should be 101.00 (from o2, earlier than o4)

    # Test retrieval by ID
    retrieved_o5 = book_a.get_order(o5.order_id)
    print(f"Retrieved o5 by ID: {retrieved_o5}")


    # Test removal (cancellation)
    print(f"\nCancelling order {o3.order_id.split('-')[0]} (Current Best Bid)...")
    removed_order = book_a.remove_order(o3.order_id)
    print(f"Order removed from map: {removed_order}") # Will show status CANCELLED
    # Try getting it again after removal from map
    print(f"Try getting o3 after removal: {book_a.get_order(o3.order_id)}") # Should be None
    print(f"Best Bid after cancel: {book_a.get_best_bid()}") # Should now be 99.00 (o1 is earlier than o5)
    book_a.display()


    # Test popping best orders (simulates matching engine taking liquidity)
    print("\nPopping best ask...")
    best_ask_order = book_a.pop_best_ask_order() # Should be o2 (price 101.0, earlier timestamp than o4)
    print(f"Popped Ask: {best_ask_order}")
    print(f"Book state after pop: {book_a}")
    print(f"Best ask after pop: {book_a.get_best_ask()}") # Should be 101.00 (now from o4)

    print("\nPopping best bid...")
    best_bid_order = book_a.pop_best_bid_order() # Should be o1 (price 99.0, earlier timestamp than o5)
    print(f"Popped Bid: {best_bid_order}")
    print(f"Book state after pop: {book_a}")
    print(f"Best bid after pop: {book_a.get_best_bid()}") # Should be 99.00 (now from o5)
    print("\nPopping next best bid...")
    next_best_bid = book_a.pop_best_bid_order() # Should be o5
    print(f"Popped Bid: {next_best_bid}")
    print(f"Best bid after pop: {book_a.get_best_bid()}") # Should be 98.00 (from o8)


    book_a.display()