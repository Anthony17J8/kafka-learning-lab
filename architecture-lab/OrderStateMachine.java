import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OrderStateMachine {

    private static final Map<Long, OrderState> orders = new HashMap<>();
    private static final Set<String> processedEvents = new HashSet<>();

    enum EventType {
        INVENTORY_RESERVED,
        PAYMENT_COMPLETED,
        ORDER_COMPLETED
    }

    enum OrderState {
        CREATED, RESERVED, PAID, COMPLETED
    }

    record Event(String eventId, long orderId, EventType eventType) {

    }

    public static boolean apply(Event event) {
        if (processedEvents.contains(event.eventId())) {
            return false;
        }

        OrderState currentState = orders.get(event.orderId());
        if (currentState == null) {
            throw new IllegalStateException("Order not found with id " + event.eventId());
        }
        OrderState newState = switch (event.eventType()) {
            case ORDER_COMPLETED -> {
                if (OrderState.PAID != currentState) {
                    throw new IllegalStateException(
                            "Invalid order state transition from " + currentState + " to " + OrderState.COMPLETED);
                } else {
                    yield OrderState.COMPLETED;
                }
            }
            case INVENTORY_RESERVED -> {
                if (OrderState.CREATED != currentState) {
                    throw new IllegalStateException(
                            "Invalid order state transition from " + currentState + " to " + OrderState.RESERVED);
                } else {
                    yield OrderState.RESERVED;
                }
            }
            case PAYMENT_COMPLETED -> {
                if (OrderState.RESERVED != currentState) {
                    throw new IllegalStateException(
                            "Invalid order state transition from " + currentState + " to " + OrderState.PAID);
                } else {
                    yield OrderState.PAID;
                }
            }
        };
        orders.put(event.orderId(), newState);
        processedEvents.add(event.eventId());
        return true;
    }

    static void main(String[] args) {
        orders.put(101L, OrderState.CREATED);

        Event e1 = new Event(
                "evt-1", 101L, EventType.INVENTORY_RESERVED
        );

        Event e2 = new Event(
                "evt-2", 101L, EventType.PAYMENT_COMPLETED
        );

        Event e3 = new Event(
                "evt-3", 101L, EventType.ORDER_COMPLETED
        );

        System.out.println(apply(e1));
        System.out.println(apply(e1)); // Duplicate
        System.out.println(apply(e2));
        System.out.println(apply(e3));

        System.out.println(orders.get(101L));

        orders.put(102L, OrderState.CREATED);

        Event invalid = new Event(
                "evt-invalid",
                102L,
                EventType.PAYMENT_COMPLETED
        );

        try {
            apply(invalid);
        } catch (IllegalStateException e) {
            System.out.println("Invalid transition rejected");
        }

        System.out.println(orders.get(102L));
        System.out.println(processedEvents.contains("evt-invalid"));
    }
}
