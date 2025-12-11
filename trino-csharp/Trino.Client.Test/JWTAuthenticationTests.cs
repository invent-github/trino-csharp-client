using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Trino.Client;
using Trino.Client.Auth;
using Trino.Data.ADO.Client;
using Trino.Data.ADO.Server;

namespace Trino.Client.Test
{
    [TestClass]
    public class JWTAuthenticationTests
    {
        /// <summary>
        /// Test JWT authentication with a real Trino server
        /// Replace YOUR_JWT_TOKEN with your actual JWT token
        /// Replace YOUR_TRINO_HOST with your Trino server host
        /// </summary>
        [TestMethod]
        public void TestJWTAuthenticationWithRealServer()
        {
            // Configuration - replace these with your actual values
            const string trinoHost = "your-trino-host"; // Replace with your host
            const string trinoPort = "443"; // Default HTTPS port
            const string jwtToken = "YOUR_JWT_TOKEN_HERE"; // Replace with your JWT token
            const bool enableSsl = true; // Use HTTPS for JWT auth
            
            // Skip test if token is not configured
            if (jwtToken == "YOUR_JWT_TOKEN_HERE")
            {
                Assert.Inconclusive("JWT token not configured. Please replace YOUR_JWT_TOKEN_HERE with your actual token.");
                return;
            }

            // Create connection properties with JWT authentication
            TrinoConnectionProperties properties = new()
            {
                Host = trinoHost,
                Port = int.Parse(trinoPort),
                EnableSsl = enableSsl,
                Auth = new TrinoJWTAuth { AccessToken = jwtToken },
                Catalog = "catalog", // Replace with your catalog
                Schema = "schema"     // Replace with your schema
            };

            try
            {
                using (TrinoConnection connection = new(properties))
                {
                    // Open the connection - this will test JWT authentication
                    connection.Open();
                    
                    // Verify connection is established
                    Assert.AreEqual(ConnectionState.Open, connection.State);
                    
                    // Execute a simple query
                    using (TrinoCommand command = new(connection, @"select 1"))
                    {
                        using (TrinoDataReader reader = (TrinoDataReader)command.ExecuteReader())
                        {
                            // Verify we can read results
                            Assert.IsTrue(reader.Read());
                            
                            // Verify the result
                            int result = reader.GetInt32(0);
                            Assert.IsNotNull(result);
                            
                            Console.WriteLine("JWT Authentication Test: Successfully executed query and got result: " + result);
                        }
                    }
                }
                
                Console.WriteLine("JWT Authentication Test: PASSED - Connection and query execution successful");
            }
            catch (Exception ex)
            {
                Assert.Fail($"JWT Authentication Test FAILED: {ex.Message}\n\nStack Trace:\n{ex.StackTrace}");
            }
        }

        /// <summary>
        /// Test JWT authentication with the specific query you requested
        /// Replace placeholders with your actual values
        /// </summary>
        [TestMethod]
        public void TestJWTSelectFromTable()
        {
            // Configuration - replace these with your actual values
            const string trinoHost = "your-trino-host.com"; // Replace with your host
            const string jwtToken = "YOUR_JWT_TOKEN_HERE"; // Replace with your JWT token
            const string catalog = "your_catalog"; // Replace with your catalog
            const string schema = "xyz"; // Your schema name
            const string table = "table"; // Your table name
            
            // Skip test if not configured
            if (jwtToken == "YOUR_JWT_TOKEN_HERE" || trinoHost == "your-trino-host.com")
            {
                Assert.Inconclusive("Please configure your Trino host and JWT token before running this test.");
                return;
            }

            TrinoConnectionProperties properties = new()
            {
                Host = trinoHost,
                Port = 443,
                EnableSsl = true,
                Auth = new TrinoJWTAuth { AccessToken = jwtToken },
                Catalog = catalog,
                Schema = schema
            };

            try
            {
                using (TrinoConnection connection = new(properties))
                {
                    connection.Open();
                    Assert.AreEqual(ConnectionState.Open, connection.State);
                    
                    // Execute your specific query
                    string query = $"SELECT 1 FROM {schema}.{table} LIMIT 1";
                    using (TrinoCommand command = new(connection, query))
                    {
                        using (TrinoDataReader reader = (TrinoDataReader)command.ExecuteReader())
                        {
                            Console.WriteLine($"JWT Query Test: Executing '{query}'");
                            
                            // Check if we got any results
                            bool hasRows = reader.Read();
                            Console.WriteLine($"JWT Query Test: Query returned results: {hasRows}");
                            
                            if (hasRows)
                            {
                                int result = reader.GetInt32(0);
                                Console.WriteLine($"JWT Query Test: First row value: {result}");
                                Assert.AreEqual(1, result);
                            }
                            else
                            {
                                Console.WriteLine("JWT Query Test: Query returned no rows (table might be empty)");
                            }
                        }
                    }
                }
                
                Console.WriteLine("JWT Query Test: PASSED - Successfully connected and executed query");
            }
            catch (Exception ex)
            {
                Assert.Fail($"JWT Query Test FAILED: {ex.Message}\n\nStack Trace:\n{ex.StackTrace}");
            }
        }

        /// <summary>
        /// Test JWT token validation
        /// </summary>
        [TestMethod]
        public void TestJWTTokenValidation()
        {
            // Test with valid token format
            string validToken = "REPLACE_YOUR_TOKEN_HERE";
            
            TrinoJWTAuth jwtAuth = new() { AccessToken = validToken };
            
            // Should not throw exception for valid token format
            jwtAuth.AuthorizeAndValidate();
            
            // Test with null token
            TrinoJWTAuth nullTokenAuth = new() { AccessToken = null };
            try
            {
                nullTokenAuth.AuthorizeAndValidate();
                Assert.Fail("Expected ArgumentException for null token");
            }
            catch (ArgumentException)
            {
                // Expected exception
            }
            
            // Test with empty token
            TrinoJWTAuth emptyTokenAuth = new() { AccessToken = "" };
            try
            {
                emptyTokenAuth.AuthorizeAndValidate();
                Assert.Fail("Expected ArgumentException for empty token");
            }
            catch (ArgumentException)
            {
                // Expected exception
            }
            
            Console.WriteLine("JWT Token Validation Test: PASSED");
        }

        /// <summary>
        /// Test JWT authorization header creation
        /// </summary>
        [TestMethod]
        public void TestJWTAuthorizationHeader()
        {
            const string testToken = "test.jwt.token";
            TrinoJWTAuth jwtAuth = new() { AccessToken = testToken };
            
            // Create a mock HTTP request to test header addition
            using (var request = new System.Net.Http.HttpRequestMessage())
            {
                jwtAuth.AddCredentialToRequest(request);
                
                // Verify Authorization header was added correctly
                Assert.IsTrue(request.Headers.Contains("Authorization"));
                var authHeader = request.Headers.Authorization;
                Assert.IsNotNull(authHeader);
                Assert.AreEqual("Bearer", authHeader.Scheme);
                Assert.AreEqual(testToken, authHeader.Parameter);
            }
            
            Console.WriteLine("JWT Authorization Header Test: PASSED");
        }
    }
}