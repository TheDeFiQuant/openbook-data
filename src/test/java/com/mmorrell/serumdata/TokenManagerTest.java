package com.mmorrell.serumdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mmorrell.serumdata.manager.TokenManager;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TokenManagerTest {

    private TokenManager tokenManager;
    
    @BeforeEach
    public void setup() throws IOException {
        OkHttpClient client = new OkHttpClient.Builder()
                .callTimeout(10, TimeUnit.SECONDS)
                .build();
        ObjectMapper objectMapper = new ObjectMapper();
    
        // Mock the ResourceLoader
        ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
    
        // Mock the Resource to return a non-empty InputStream to simulate an actual image being loaded
        Resource mockResource = Mockito.mock(Resource.class);
        
        // Simulate a non-empty image file - you can adjust the size of the byte array as needed
        byte[] nonEmptyImageBytes = new byte[10]; // Simulate non-empty content
        Arrays.fill(nonEmptyImageBytes, (byte) 1); // Fill with dummy data
        ByteArrayInputStream mockInputStream = new ByteArrayInputStream(nonEmptyImageBytes);
    
        Mockito.when(mockResource.getInputStream()).thenReturn(mockInputStream);
    
        // When getResource is called with any String, return the mockResource
        Mockito.when(resourceLoader.getResource(Mockito.anyString())).thenReturn(mockResource);
    
        // Initialize TokenManager with mocked dependencies
        tokenManager = new TokenManager(client, objectMapper, resourceLoader);
    }
        

    @Test
    public void constructorTest() {
        // This test asserts that the placeholder image is loaded without errors
        assertTrue(tokenManager.getPlaceHolderImage().length > 0, "Placeholder image should be loaded");
    }
}
