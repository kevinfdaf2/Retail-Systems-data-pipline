# **Project part 6**

There are two steps for this part. First, we need to build out a model, typically this is a data scientist's job, and they will fit the model locally to find the best parameter. Then we can use the parameter to deploy the best model on the cloud.

# Build XGBoost model in R [main.R](./model/r/main.R)

## Add new features for training

```
data$prod_reorder_probability <- data$prod_second_orders / data$prod_first_orders
data$prod_reorder_times <- 1 + data$prod_reorders / data$prod_first_orders
data$prod_reorder_ratio <- data$prod_reorders / data$prod_orders
data$user_average_basket <- data$user_total_products / data$user_orders
data$up_order_rate <- data$up_orders / data$user_orders
data$up_orders_since_last_order <- data$user_orders - data$up_last_orders
data$up_order_rate_since_first_order <- data$up_orders / (data$user_orders - data$up_first_orders + 1)
```

## Set hyper parameter grid

```
hyper_grid <-expand.grid(
  nrounds =               c(3000),
  objective =             c("binary:logistic"),
  eval_metric =           c("auc"),
  max_depth =             c(6),
  eta =                   c(0.1,0.05),
  gamma =                 c(0.7,0.5),
  colsample_bytree =      c(0.95),
  subsample =             c(0.75),
  min_child_weight =      c(10),
  alpha =                 c(2e-05),
  lambda =                c(10),
  scale_pos_weight =      c(1)
)
```

## Use cross validation to find the best parameter


```
for(i in 1:nrow(hyper_grid)){
  cat(paste0("\nModel ", i, " of ", nrow(hyper_grid), "\n"))
  cat("Hyper-parameters:\n")
  print(hyper_grid[i,])
  
  metricsValidComb <- data.frame()
  
  
  cv.nround = 100
  cv.nfold = 3
  mdcv <- xgb.cv(data=train_data, params = as.list(hyper_grid[i,]), nthread=6, 
                 nfold=cv.nfold, nrounds=cv.nround,
                 verbose = T)
  
  model_auc = min(mdcv$evaluation_log$test_auc_mean)
  
  # make prediction on the validation dataset
  #evaluation <- predict(model, newdata = valid_data_x)
  # calculate AUC based on the prediction result
  #metrics <- roc(as.vector(valid_data_y), evaluation)
  # get the auc value
  #model_auc <- as.numeric(metrics$auc)
  # put together AUC and the best iteration value
  metrics_frame <- data.frame(AUC = model_auc)
  # combine the result for each fold
  #metricsValidComb <- rbind(metricsValidComb, metrics_frame)
  final_valid_metrics <- rbind(final_valid_metrics, metrics_frame)
  cat(paste0("AUC: ", round(model_auc, 3), "\n"))
  
}
```

# Deploy the model on Segemaker
![](./model-deployment.jpeg)

















For both schedule based and trigger based we need same Lambda funcitons to trigger Databrew jobs and Glue jobs, but the trigger for lambda is different. One is base on S3 upload event notification, the other is scheduled on Eventbidge.

## Lambda function for Databrew

- name: databrew
- runtime: python 3.8
- IAM role: full access to Databrew
- Timeout: 15 sec


```python
import boto3
databrew= boto3.client('databrew')
def lambda_handler(event, context):    
    response = databrew.start_job_run(Name='prd-features-job')
    response = databrew.start_job_run(Name='user-features-2-job')
    response = databrew.start_job_run(Name='up-features-job')
    response = databrew.start_job_run(Name='user-features-1-job')
    
```

## Lambda function for Glue job

- name: glue-job
- runtime: python 3.8
- IAM role: full access to Glue
- Timeout: 10 sec


```python
import boto3
glue= boto3.client('glue')
def lambda_handler(event, context):    
    response = glue.start_job_run(JobName='imba-glue-job')
```


# Schedule Based ETL Pipline

![](./AutomatedETLPipeline-ScheduleBased.jpeg)

After bulid two Lambda funtion we can create EventBridge to trigger it.

## Rule for trigger Databrew:

- name: databrew-etl-daily
- Rule type: schedule
- pattern: fine-grained schedule (Cron)
- target: lambda functon (databrew)

## Rule for trigger Glue Job:

- name: glue-job-etl-daily
- Rule type: schedule
- pattern: fine-grained schedule (Cron)
- target: lambda functon (glue-job)

![](./steps/enventbridge-databrew-schedule.png)


### We use trigger based for now
![](./steps/enventbridge-status.png)


# Trigger Based ETL Pipline

![](./AutomatedETLPipeline-TriggerBased.jpeg)

### Base on the Lambda function, we create event notification

![](./steps/event-notifications-setups.png)


