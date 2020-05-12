#################################################
#                                               #
#    1. Create edx set, validation set          #
#                                               #
#################################################

# Note: this process could take a couple of minutes

#load required packages
library(Hmisc)
library(tidyverse)
library(data.table)
library(caret)
library(doParallel) 
library(lubridate)


#activate parallel computing to avoid CPU-bound limitations
cl <- makeCluster(detectCores(), type='PSOCK')
registerDoParallel(cl)

# MovieLens 10M dataset:
# https://grouplens.org/datasets/movielens/10m/
# http://files.grouplens.org/datasets/movielens/ml-10m.zip

dl <- tempfile()
download.file("http://files.grouplens.org/datasets/movielens/ml-10m.zip", dl)

ratings <- fread(text = gsub("::", "\t", readLines(unzip(dl, "ml-10M100K/ratings.dat"))),
                 col.names = c("userId", "movieId", "rating", "timestamp"))

movies <- str_split_fixed(readLines(unzip(dl, "ml-10M100K/movies.dat")), "\\::", 3)
colnames(movies) <- c("movieId", "title", "genres")
movies <- as.data.frame(movies, stringsAsFactors =TRUE) %>% mutate(movieId = as.numeric(levels(movieId))[movieId],
                                           title = as.character(title),
                                           genres = as.character(genres))


movielens <- left_join(ratings, movies, by = "movieId")

#### Transformation of the movielens dataset
movielens_trans <- movielens %>% 
  # extract year from title column
  mutate(year = as.numeric(str_sub(title,-5,-2)))%>% 
  # calculate age of movies 
  mutate(age = 2009-year) %>% 
  # change timestamp to date (week format)
  mutate(date = as_datetime(timestamp)) %>%
  mutate(date = round_date(date, unit = "week"))

# add average number of ratings/year to each movie
movielens_trans <- movielens_trans %>% 
  # calculate average number of ratings/year
  group_by(movieId) %>%
  summarize(n = n(), years = 2009 - first(year),
            title = title[1],
            rating = mean(rating)) %>%
  mutate(rate = n/years) %>% 
  # add Rate to the existing movielens data
  select(movieId, rate) %>%
  right_join(movielens_trans,by="movieId")

# Validation set will be 10% of MovieLens data
set.seed(1, sample.kind="Rounding")
# if using R 3.5 or earlier, use `set.seed(1)` instead
test_index <- createDataPartition(y = movielens$rating, times = 1, p = 0.1, list = FALSE)
edx <- movielens_trans[-test_index,]
temp <- movielens_trans[test_index,]

# Make sure userId and movieId in validation set are also in edx set
validation <- temp %>% 
  semi_join(edx, by = "movieId") %>%
  semi_join(edx, by = "userId")

# Add rows removed from validation set back into edx set
removed <- anti_join(temp, validation)
edx <- rbind(edx, removed)

rm(dl, ratings, movies, test_index, temp, movielens, movielens_trans, removed)




#################################################
#                                               #
#      2. Exploratory Data Analysis             #
#                                               #
#################################################

# view first rows of the data
head(edx)

# summary
summary(edx)

# number of unique movies and users
edx %>%
  summarize(n_users = n_distinct(userId),
            n_movies = n_distinct(movieId),
            n_genres = n_distinct(genres))

# mean rating
mu <- edx %>% 
  summarize(mean = mean(rating)) %>% pull(mean)

# distribution of movie ratings
edx %>%
  group_by(rating) %>%
  ggplot(aes(rating)) + 
  geom_histogram(bins = 30, color = "black") +
  theme_bw() +
  scale_y_continuous(breaks = seq(0,2500000,by=500000)) +
  ggtitle("Rating distribution")


########################## 
# Inspect Movie variable #
##########################


# distribution of number of ratings per movieId 
edx %>%
  count(movieId) %>%
  ggplot(aes(n)) + 
  geom_histogram(bins = 30, color = "black") +
  scale_x_log10()+
  theme_bw() +
  xlab("Number of ratings") +
  ylab("Number of movies") +
  ggtitle("Number of ratings per movie")

# movie effect
edx %>% 
  group_by(movieId) %>% 
  summarize(b_movie = mean(rating)) %>% 
  filter(n()>=100) %>%
  ggplot(aes(b_movie)) + 
  geom_histogram(bins = 30, color = "black") +
  geom_vline(xintercept = mu, color = "red", lty = 2) +
  theme_bw() +
  xlab("Average rating") +
  ylab("Number of movies") +
  ggtitle("Distribution of average rating per movie")


########################## 
# Inspect User variable #
##########################

# distribution of userIds 
edx %>%
  count(userId) %>%
  ggplot(aes(n)) + 
  geom_histogram(bins = 30, color = "black") +
  scale_x_log10()+
  theme_bw() +
  xlab("Number of ratings") +
  ylab("Number of users") +
  ggtitle("Number of ratings per user")

# user effect
edx %>% 
  group_by(userId) %>% 
  summarize(b_user = mean(rating)) %>% 
  filter(n()>=100) %>%
  ggplot(aes(b_user)) + 
  geom_histogram(bins = 30, color = "black") +
  geom_vline(xintercept = mu, color = "red", lty = 2)+
  theme_bw() +
  xlab("Average rating") +
  ylab("Number of users") +
  ggtitle("Distribution of average movie rating per user")


########################## 
# Inspect Age of movies  #
##########################

# Age effect
edx %>% group_by(age) %>%
  summarize(rating = mean(rating)) %>%
  ggplot(aes(age, rating)) +
  geom_point() +
  geom_smooth()  + 
  geom_hline(yintercept = mu, lty =2,color="red") +
  theme_bw() +
  scale_x_continuous(breaks = seq(0,100, by=10)) +
  xlab("Age of movie (years)") +
  ylab("Rating") +
  ggtitle("Effect of movie age on rating")


####################################### 
# Inspect Rate of movies               #
# (number of ratings/year per movies) #
#######################################

#### Inspect Rate (number of ratings/year per movies)
# check for movies with the highest number of ratings per year
most_rated <- edx %>%
  group_by(title) %>%
  summarize(mean_rate = mean(rate),
            mean_rating= mean(rating)) %>%
  top_n(25, mean_rate) %>%
  arrange(desc(mean_rate)) 


# check for movies with the lowest number of ratings per year
least_rated <- edx %>%
  group_by(title) %>%
  summarize(mean_rate = mean(rate),
            mean_rating= mean(rating), n=n()) %>%
  top_n(-25, mean_rate) %>%
  arrange(desc(mean_rate))  

# compare average rating of most 25 and least 25 rated movies
data.frame(most_rated = mean(most_rated$mean_rating), 
           least_rated =mean(least_rated$mean_rating)) 

# Rate effect
edx %>% 
  group_by(movieId) %>%
  summarize(mean_rate = mean(rate),
            mean_rating = mean(rating)) %>%
  ggplot(aes(mean_rate, mean_rating)) +geom_point()+
  geom_smooth() + 
  geom_hline(yintercept = mu, lty =2,color="red") +
  theme_bw() + scale_x_continuous(breaks = seq(0,2000,200))+
  xlab("Rate (ratings/year)")+
  ylab("Average movie rating") +
  ggtitle("Average rating per movie vs. Ratings/year per movie")


######################################
# Inspect date of rating submission  #
######################################

# show bias based on when the data was submitted (time effect)
edx %>% 
  group_by(date) %>%
  summarize(rating = mean(rating)) %>%
  ggplot(aes(date, rating)) +
  geom_point() +
  geom_smooth() + 
  geom_hline(yintercept = mu, lty =2,color="red") +
  theme_bw()+
  xlab("Average rating") +
  ylab("Week of rating submission") +
  ggtitle("Fluctuations in average rating based on submission date")


##########################################################
# Inspect Genre variable                                 #                                                      #
#   as str_split on the whole data.frame requires too    #
#   much RAM, I used a workaround  to extract mean       #
#   and 95% ci for each respective genre                 #
##########################################################

# extract genre names
genre_names <- unique(edx$genres) %>%
  str_split("\\|") %>%
  unlist() %>%
  unique() %>%
  .[-length(.)] # remove value "(no genres listed)"

# apply function over each genre to calculate mean and 95% ci
stat_genre <- sapply(genre_names, function(x){
edx %>% 
  filter(str_detect(edx$genres, x)) %>%
    # the confidence intervals could also be calculated by a bootstrap approach but were #
    # using way to much computation time. Although data is not normal, based on CLT this #
    # approximation of the cis should work well                                          #
  summarize(ci = list(mean_cl_normal(rating))) %>%
  unlist()
})%>%
  t() %>%
  data.frame() %>%
  rownames_to_column("Genre")

# plot mean with 95% confidence interval
stat_genre %>% mutate(Genre = reorder(Genre, ci.y)) %>%
    ggplot(aes(Genre, ci.y)) +
    geom_point(size=1) +
    geom_errorbar(aes(ymin = ci.ymin, ymax = ci.ymax),width=0.5)  +
    geom_hline(yintercept = mu, lty =2,color="red") +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90)) +
    ylab("Average rating (+ 95% CI)") +
    ggtitle("Average rating by genre")









#####################################################################
#                                                                   #
#      3. Modelling Approach: Model tuning and Model selection      #
#                                                                   # 
#####################################################################

# split edx data into training and test set for parameter tuning 
set.seed(12)
test_index <- createDataPartition(y = edx$rating, times = 1, p = 0.1, list = FALSE)
train_set <- edx[-test_index,]
temp <- edx[test_index,]

# Make sure userId and movieId in test set are also in training set
test_set <- temp %>% 
  semi_join(train_set, by = "movieId") %>%
  semi_join(train_set, by = "userId")

# Add rows removed from test set back into training set
removed <- anti_join(temp, test_set)
train_set <- rbind(train_set, removed)

rm(temp, removed)

# define function to calculate RMSE
RMSE <- function(true_ratings, predicted_ratings){
sqrt(mean((true_ratings - predicted_ratings)^2))
}

#calculate mean of train set for modeling purposes
mu <- train_set %>% 
  summarize(mean = mean(rating)) %>% pull(mean)


########################
# 3.1: Baseline Model  #
########################

# predict all values to be mean mu of training set 
mu_hat <- mu
naive_rmse <- RMSE(test_set$rating, mu_hat)
naive_rmse

# show results in table
rmse_results <- data_frame(method = "Just the average", RMSE = naive_rmse)
rmse_results %>% knitr::kable()


########################
# 3.2: Movie Model     #
########################

# define model taking movie bias into account
movie_avgs <- train_set %>% 
  group_by(movieId) %>% 
  summarize(b_movie = mean(rating - mu))

# prediction of ratings based on movie bias
predicted_ratings <- mu + test_set %>% 
  left_join(movie_avgs, by='movieId') %>%
  .$b_movie

# calculate RMSE for movie model
model_1_rmse <- RMSE(test_set$rating, predicted_ratings)

# bind results to rmse table
rmse_results <- bind_rows(rmse_results,
                          data_frame(method="Movie Effect Model",
                                     RMSE = model_2_rmse ))
rmse_results %>% knitr::kable()


###########################
# 3.3: Movie + User Model #
###########################

# define model taking movie and user bias into account
user_avgs <- train_set %>% 
  left_join(movie_avgs, by='movieId') %>%
  group_by(userId) %>%
  summarize(b_user = mean(rating - mu - b_movie))

# prediction of ratings based on movie and user bias
predicted_ratings <- test_set %>% 
  left_join(movie_avgs, by='movieId') %>%
  left_join(user_avgs, by='userId') %>%
  mutate(pred = mu + b_movie + b_user) %>%
  .$pred

# calculate RMSE for movie model
model_3_rmse <- RMSE(test_set$rating, predicted_ratings)

# bind results to rmse table
rmse_results <- bind_rows(rmse_results,
                          data_frame(method="Movie + User Effects Model",  
                                     RMSE = model_3_rmse ))
rmse_results %>% knitr::kable()



#######################################
# 3.4: Regularized Movie + User Model #
#######################################

# tuning hyperparameter lambda using training set
lambdas <- seq(0, 7, 0.25)

rmses <- sapply(lambdas,function(l){
  
  #Calculate the mean of ratings from the train_set training set
  mu <- mean(train_set$rating)
  
  #Adjust mean by movie effect and penalize low number on ratings
  b_i <- train_set %>% 
    group_by(movieId) %>%
    summarize(b_i = sum(rating - mu)/(n()+l))
  
  #ajdust mean by user and movie effect and penalize low number of ratings
  b_u <- train_set %>% 
    left_join(b_i, by="movieId") %>%
    group_by(userId) %>%
    summarize(b_u = sum(rating - b_i - mu)/(n()+l))
  
  #predict ratings in the training set to derive optimal penalty value 'lambda'
  predicted_ratings <-   test_set %>% 
    left_join(b_i, by = "movieId") %>%
    left_join(b_u, by = "userId") %>%
    mutate(pred = mu + b_i + b_u) %>%
    .$pred
  
  return(RMSE(test_set$rating, predicted_ratings))
})

# plot rmses vs lambdas
data.frame(lambda = lambdas, rmse = rmses) %>% 
  ggplot(aes(lambda, rmse)) + 
  geom_point() +
  theme_bw() + 
  ylab("RMSE") + 
  ggtitle("Tuning of regularization parameter lambda")

# extract best value lambda
model_4_lambda <- lambdas[which.min(rmses)]

# extract RMSE of the model using the optimal lambda
model_4_rmse <- min(rmses)

# bind results to the RMSE table
rmse_results <- bind_rows(rmse_results,
                          data_frame(method="Regularized Movie + User Effect Model",  
                                     RMSE = model_4_rmse ))
rmse_results %>% knitr::kable()


#########################################################################
# 3.5: Regularized Movie + User + Age + Genre+ Rate + Time Effect Model #
#########################################################################

# tuning hyperparameter lambda using training set
lambdas <- seq(0, 7, 0.25)

rmses <- sapply(lambdas,function(l){
  
  #Calculate the mean of ratings from the train_set training set
  mu <- mean(train_set$rating)
  
  #Adjust mean by movie effect and penalize low number on ratings
  b_i <- train_set %>% 
    group_by(movieId) %>%
    summarize(b_i = sum(rating - mu)/(n()+l)) 
  
  #Ajdust mean by user and movie effect and penalize low number of ratings
  b_u <- train_set %>% 
    left_join(b_i, by="movieId") %>%
    group_by(userId) %>%
    summarize(b_u = sum(rating - b_i - mu)/(n()+l))
  
  #Ajdust mean by genre, user and movie effect and penalize low number of ratings
  b_g <- train_set %>% 
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    group_by(genres) %>%
    summarize(b_g = sum(rating - b_i - b_u - mu)/(n()+l))
  
  #Ajdust mean by age, genre, user and movie effect and penalize low number of ratings
  b_a <- train_set %>% 
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    group_by(age) %>%
    summarize(b_a = sum(rating - b_i - b_u- b_g - mu)/(n()+l))
  
  #Ajdust mean by rate, time, age, genre, user and movie effect and penalize low number of ratings
  b_r <- train_set %>% 
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    left_join(b_a, by="age") %>%
    group_by(rate,movieId) %>%
    summarize(b_r = sum(rating - b_i - b_u- b_g - b_a- mu)/(n()+l)) 
  
  #Ajdust mean by time, age, genre, user and movie effect and penalize low number of ratings
  b_t <- train_set %>% 
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    left_join(b_a, by="age") %>%
    left_join(b_r, by="movieId")  %>%
    group_by(date) %>%
    summarize(b_t = sum(rating - b_i - b_u- b_g - b_a - b_r - mu)/(n()+l))
  
  test_set %>% 
    left_join(b_i, by="movieId") %>% filter(is.na(b_i)) %>% .$b_i %>% sum()
  
  #predict ratings in the training set to derive optimal penalty value 'lambda'
  predicted_ratings <- test_set %>% 
    left_join(b_i, by="movieId") %>% mutate(b_i = ifelse(is.na(b_i),0,b_i)) %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    left_join(b_a, by="age") %>%
    left_join(b_r, by="movieId") %>%mutate(b_r = ifelse(is.na(b_r),0,b_r)) %>%
    left_join(b_t, by="date") %>%
    mutate(pred = mu + b_i + b_u + b_g +b_a+b_r+b_t) %>%
    .$pred
  
  return(RMSE(test_set$rating, predicted_ratings))
})


# plot rmses vs lambdas
data.frame(lambda = lambdas, rmse = rmses) %>% 
  ggplot(aes(lambda, rmse)) + 
  geom_point() +
  theme_bw() + 
  ylab("RMSE") + 
  ggtitle("Tuning of regularization parameter lambda")

# extract optimal lambda
model_5_lambda <- lambdas[which.min(rmses)]

# extract RMSE of the model using the optimal lambda
model_5_rmse <- min(rmses)

# bind results to the RMSE table
rmse_results <- bind_rows(rmse_results,
                          data_frame(method="Regularized  Movie + User + Genre +
                                     Age + Rate + Time Effect Model",  
                                     RMSE = model_5_rmse ))
rmse_results %>% knitr::kable()









###############################################################
#                                                             #
#                 4. Validation of the final model            #
#                                                             # 
###############################################################

# Final Model: Regularized Movie + User + Age+ Genre+ Rate Effect Model

# optimal tuning parameter lambda (acquired in paragraph 3)
lambda <- model_5_lambda

# calculate RMSE on validation set
predicted_ratings <- sapply(lambda,function(l){
  
  #Calculate the mean of ratings from the train_set training set
  mu <- mean(edx$rating)
  
  #Adjust mean by movie effect and penalize low number on ratings
  b_i <- edx %>% 
    group_by(movieId) %>%
    summarize(b_i = sum(rating - mu)/(n()+l))
  
  #Ajdust mean by user and movie effect and penalize low number of ratings
  b_u <- edx %>% 
    left_join(b_i, by="movieId") %>%
    group_by(userId) %>%
    summarize(b_u = sum(rating - b_i - mu)/(n()+l))
  
  #Ajdust mean by genre, user and movie effect and penalize low number of ratings
  b_g <- edx %>% 
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    group_by(genres) %>%
    summarize(b_g = sum(rating - b_i - b_u - mu)/(n()+l))
  
  # Ajdust mean by age, genre, user and movie effect and penalize low number of ratings
  b_a <- edx %>%
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    group_by(age) %>%
    summarize(b_a = sum(rating - b_i - b_u- b_g - mu)/(n()+l))
  
  # Ajdust mean by rate effect and penalize low number of ratings
  b_r <- edx %>%
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    left_join(b_a, by="age") %>%
    group_by(rate,movieId) %>% 
    summarize(b_r = sum(rating - b_i - b_u- b_g - b_a- mu)/(n()+l))
  
  # Ajdust mean by time effect and penalize low number of ratings
  b_t <- edx %>%
    left_join(b_i, by="movieId") %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    left_join(b_a, by="age") %>%
    left_join(b_r, by=c("rate","movieId"))%>%
    group_by(date) %>%
    summarize(b_t = sum(rating - b_i - b_u- b_g - b_a - b_r - mu)/(n()+l))
  
  #predict ratings in the validation set with the final value of lambda
  predicted_ratings <- validation %>%
    left_join(b_i, by="movieId") %>%  #mutate(b_i = ifelse(is.na(b_i),0,b_i)) %>%
    left_join(b_u, by="userId") %>%
    left_join(b_g, by="genres") %>%
    left_join(b_a, by="age") %>%
    left_join(b_r, by="movieId") %>%  #mutate(b_r = ifelse(is.na(b_r),0,b_r)) %>%
    left_join(b_t, by="date") %>%
    mutate(pred = mu +  b_i + b_u + b_g + b_a+ b_r + b_t) %>%
    .$pred
  
  return(predicted_ratings)
})

#calculate RMSE
model_5_validation  <- RMSE(validation$rating, predicted_ratings)

# bind results to the RMSE table
rmse_results <- bind_rows(rmse_results,
                          data_frame(method="Validation: Regularized  Movie + User + 
                                     Genre + Age + Rate + Time Effect Model",  
                                     RMSE = model_5_validation))
rmse_results %>% knitr::kable()

# Improvement over baseline model
(1 - rmse_results$RMSE[6]/rmse_results$RMSE[1]) *100 #percent



# calculate correlation of predicted and observed ratings
corr <- cor(validation$rating, predicted_ratings) 
colnames(corr) <- "pearson correlation coefficent"
corr %>% round(3) %>% knitr::kable()

# plot correlation of predicted and observed ratings (for visualization purposes: only the first 10000 entries used)
validation[1:10000,] %>% 
  mutate(pred = predicted_ratings[1:10000]) %>% 
  ggplot(aes(rating, pred)) + geom_jitter() +
  geom_smooth(method="lm") + 
  theme_bw()

# session info
sessioninfo::session_info()





    