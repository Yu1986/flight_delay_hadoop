public class Flight {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: java -jar flight.jar input intermedia output");
            return;
        }
        String[] mapFlghtInfoArgs = {args[0], args[1]};
        String[] topDelayArgs = {args[1], args[2]};
        FlightInfo.main(mapFlghtInfoArgs);
        TopDelay.main(topDelayArgs);
    }
}
