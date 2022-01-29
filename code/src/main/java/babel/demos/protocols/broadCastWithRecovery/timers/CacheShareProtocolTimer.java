package babel.demos.protocols.broadCastWithRecovery.timers;

import babel.timer.ProtocolTimer;

public class CacheShareProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 105;

    public CacheShareProtocolTimer() {
        super(CacheShareProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
