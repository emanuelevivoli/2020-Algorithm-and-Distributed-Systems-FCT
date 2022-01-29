package babel.demos.protocols.broadCastWithRecovery.timers;

import babel.timer.ProtocolTimer;

public class CacheRefreshProtocolTimer extends ProtocolTimer {

    public static final short TimerCode = 104;

    public CacheRefreshProtocolTimer() {
        super(CacheRefreshProtocolTimer.TimerCode);
    }

    @Override
    public Object clone() {
        return this;
    }
}
