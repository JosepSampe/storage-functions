package com.urv.blackeagle.runtime.function;

import com.urv.blackeagle.runtime.api.Api;
import com.urv.blackeagle.runtime.context.Context;

public interface IFunction {
	public void invoke(Context ctx, Api api);
}
