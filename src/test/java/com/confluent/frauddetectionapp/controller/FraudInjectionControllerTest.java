package com.confluent.frauddetectionapp.controller;

import com.confluent.frauddetectionapp.producer.TransactionProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class FraudInjectionControllerTest {

    @Mock
    private TransactionProducer producer;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders
                .standaloneSetup(new FraudInjectionController(producer))
                .build();
    }

    @Test
    void injectVelocity_returnsInjectedStatus() throws Exception {
        when(producer.injectVelocityBurst()).thenReturn(List.of("tx-1", "tx-2", "tx-3"));

        mockMvc.perform(post("/api/demo/inject/velocity"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("injected"))
                .andExpect(jsonPath("$.pattern").value("velocity"))
                .andExpect(jsonPath("$.transactionIds").isArray());

        verify(producer).injectVelocityBurst();
        verify(producer, never()).injectGeoMismatch();
    }

    @Test
    void injectGeo_returnsInjectedStatus() throws Exception {
        when(producer.injectGeoMismatch()).thenReturn(List.of("tx-A", "tx-B"));

        mockMvc.perform(post("/api/demo/inject/geo"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("injected"))
                .andExpect(jsonPath("$.pattern").value("geo"))
                .andExpect(jsonPath("$.transactionIds").isArray());

        verify(producer).injectGeoMismatch();
        verify(producer, never()).injectVelocityBurst();
    }

    @Test
    void injectUnknownPattern_returnsErrorMessage() throws Exception {
        mockMvc.perform(post("/api/demo/inject/bogus"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("unknown pattern, use 'velocity' or 'geo'"));

        verify(producer, never()).injectVelocityBurst();
        verify(producer, never()).injectGeoMismatch();
    }

    @Test
    void injectVelocity_transactionIdsMatchProducerOutput() throws Exception {
        List<String> expected = List.of("id-1", "id-2", "id-3", "id-4", "id-5");
        when(producer.injectVelocityBurst()).thenReturn(expected);

        mockMvc.perform(post("/api/demo/inject/velocity"))
                .andExpect(jsonPath("$.transactionIds[0]").value("id-1"))
                .andExpect(jsonPath("$.transactionIds[4]").value("id-5"));
    }
}
