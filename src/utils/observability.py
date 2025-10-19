"""
OpenTelemetry distributed tracing and observability

Provides:
- Distributed tracing across services
- Metrics collection
- Context propagation
"""

from typing import Optional, Dict, Any
from functools import wraps
import time

try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    print("OpenTelemetry not installed. Run: pip install opentelemetry-api opentelemetry-sdk")

class ObservabilityManager:
    """Manage distributed tracing and metrics"""
    
    def __init__(self, service_name: str = "banking-data-ai"):
        self.service_name = service_name
        self.tracer = None
        self.meter = None
        
        if OTEL_AVAILABLE:
            self._setup_tracing()
            self._setup_metrics()
    
    def _setup_tracing(self):
        """Setup OpenTelemetry tracing"""
        resource = Resource.create({"service.name": self.service_name})
        
        # Create tracer provider
        tracer_provider = TracerProvider(resource=resource)
        
        # Add span processor (export to collector)
        # In production, configure OTLP endpoint
        # span_processor = BatchSpanProcessor(OTLPSpanExporter())
        # tracer_provider.add_span_processor(span_processor)
        
        trace.set_tracer_provider(tracer_provider)
        self.tracer = trace.get_tracer(__name__)
    
    def _setup_metrics(self):
        """Setup OpenTelemetry metrics"""
        meter_provider = MeterProvider()
        metrics.set_meter_provider(meter_provider)
        self.meter = metrics.get_meter(__name__)
    
    def trace_function(self, span_name: Optional[str] = None):
        """Decorator to trace function execution"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if not OTEL_AVAILABLE or not self.tracer:
                    return func(*args, **kwargs)
                
                name = span_name or f"{func.__module__}.{func.__name__}"
                
                with self.tracer.start_as_current_span(name) as span:
                    # Add function attributes
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)
                    
                    start_time = time.time()
                    
                    try:
                        result = func(*args, **kwargs)
                        span.set_attribute("function.status", "success")
                        return result
                    except Exception as e:
                        span.set_attribute("function.status", "error")
                        span.set_attribute("error.type", type(e).__name__)
                        span.set_attribute("error.message", str(e))
                        span.record_exception(e)
                        raise
                    finally:
                        execution_time = time.time() - start_time
                        span.set_attribute("function.duration_seconds", execution_time)
            
            return wrapper
        return decorator

# Global observability manager
observability = ObservabilityManager()

# Convenience decorator
trace = observability.trace_function

# Example usage
if __name__ == "__main__":
    @trace("process_transaction")
    def process_transaction(transaction_id: str, amount: float):
        """Example traced function"""
        time.sleep(0.1)  # Simulate processing
        return f"Processed {transaction_id}: ${amount}"
    
    result = process_transaction("TXN-123", 100.50)
    print(result)

