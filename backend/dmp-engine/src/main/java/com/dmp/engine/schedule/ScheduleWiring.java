package com.dmp.engine.schedule;

import com.dmp.application.service.ScheduleService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

/**
 * Connects schedule changes to the scheduler.
 *
 * <p>Exists so {@link ScheduleService} can stay free of Quartz. The service manages records and
 * announces changes; this hands them to {@link ScheduleRegistrar}. A deployment without a scheduler
 * simply never registers this and schedule records are still managed correctly — the same
 * substitutability every outbound port here has.
 */
@Component
@Profile({"control-plane", "all", "default"})
public class ScheduleWiring {

    private final ScheduleService service;
    private final ScheduleRegistrar registrar;

    public ScheduleWiring(ScheduleService service, ScheduleRegistrar registrar) {
        this.service = service;
        this.registrar = registrar;
    }

    @PostConstruct
    void connect() {
        service.onChange(registrar::register, registrar::unregister);
    }
}
