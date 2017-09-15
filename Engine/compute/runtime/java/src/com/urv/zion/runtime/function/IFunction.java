package com.urv.zion.runtime.function;

import com.urv.zion.runtime.api.Api;
import com.urv.zion.runtime.context.Context;

public interface IFunction {
	public void invoke(Context ctx, Api api);
}
