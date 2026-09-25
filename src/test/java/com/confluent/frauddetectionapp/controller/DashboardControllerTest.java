package com.confluent.frauddetectionapp.controller;

import com.confluent.frauddetectionapp.service.AlertBroadcaster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class DashboardControllerTest {

    @Mock
    private AlertBroadcaster broadcaster;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders
                .standaloneSetup(new DashboardController(broadcaster))
                .build();
    }

    @Test
    void stream_returns200() throws Exception {
        when(broadcaster.subscribe()).thenReturn(new SseEmitter(0L));

        mockMvc.perform(get("/api/dashboard/stream"))
                .andExpect(status().isOk());

        verify(broadcaster).subscribe();
    }

    @Test
    void stream_contentTypeIsEventStream() throws Exception {
        when(broadcaster.subscribe()).thenReturn(new SseEmitter(0L));

        mockMvc.perform(get("/api/dashboard/stream"))
                .andExpect(content().contentTypeCompatibleWith("text/event-stream"));
    }

    @Test
    void stream_callsSubscribeOnEachRequest() throws Exception {
        when(broadcaster.subscribe()).thenReturn(new SseEmitter(0L));

        mockMvc.perform(get("/api/dashboard/stream"));
        mockMvc.perform(get("/api/dashboard/stream"));
        mockMvc.perform(get("/api/dashboard/stream"));

        verify(broadcaster, times(3)).subscribe();
    }
}
