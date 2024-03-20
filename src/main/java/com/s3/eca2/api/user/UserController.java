package com.s3.eca2.api.user;

import com.s3.eca2.domain.user.User;
import com.s3.eca2.domain.user.UserService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/api/v1/s3/user")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService){
        this.userService = userService;
    }

    @GetMapping("/{userEid}")
    public User selectOne(@PathVariable long userEid){
        return userService.find(userEid);
    }
}
