package org.apache.impala.observe;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.impala.thrift.TOtelTraceConfigs;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;

public class ImpalaQueryTracer implements Tracer {

  private final TOtelTraceConfigs cfg_;

  public ImpalaQueryTracer(final TOtelTraceConfigs cfg) {
    this.cfg_ = Objects.requireNonNull(cfg);
  }
  
  @Override
  public SpanBuilder spanBuilder(@Nonnull String spanName) {
    throw new UnsupportedOperationException("Unimplemented method 'spanBuilder'");
  }
  
}
