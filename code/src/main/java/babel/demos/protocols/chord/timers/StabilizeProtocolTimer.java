package babel.demos.protocols.chord.timers;

import babel.timer.ProtocolTimer;

public class StabilizeProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 501;

    public StabilizeProtocolTimer() {
        super(StabilizeProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
