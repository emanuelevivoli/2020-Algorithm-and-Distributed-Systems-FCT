package babel.demos.protocols.hyParView.timers;

import babel.timer.ProtocolTimer;

public class ShuffleProtocolTimer  extends ProtocolTimer {

    public static final short TimerCode = 101;

    public ShuffleProtocolTimer() {
        super(ShuffleProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
